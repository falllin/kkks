package io.nexusstore.raft;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Small bounded binary image: avoids base64 expansion inside the transferred image. */
final class SnapshotCodec {
    private static final int MAGIC = 0x4e535331; // NSS1
    private SnapshotCodec() { }

    static byte[] encode(SnapshotImage image, int limit) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(new FilterOutputStream(bytes) {
            private int written;
            @Override public void write(int value) throws IOException {
                reserve(1); this.out.write(value);
            }
            @Override public void write(byte[] value, int offset, int length) throws IOException {
                reserve(length); this.out.write(value, offset, length);
            }
            private void reserve(int length) throws IOException {
                if ((long) written + length > limit) throw new IOException("Snapshot exceeds configured byte limit");
                written += length;
            }
        });
        out.writeInt(MAGIC);
        writeText(out, image.groupId());
        out.writeInt(image.members().size());
        for (String member : image.members()) writeText(out, member);
        out.writeLong(image.index()); out.writeLong(image.term());
        out.writeInt(image.values().size());
        for (String key : new TreeSet<>(image.values().keySet())) {
            writeText(out, key);
            byte[] value = image.values().get(key);
            out.writeInt(value.length); out.write(value);
        }
        out.writeInt(image.requests().size());
        for (String id : new TreeSet<>(image.requests().keySet())) {
            SnapshotImage.AppliedRequest request = image.requests().get(id);
            writeText(out, id); out.write(HexFormat.of().parseHex(request.fingerprint()));
            out.writeBoolean(request.result().found()); out.writeLong(request.result().index());
        }
        out.flush();
        return bytes.toByteArray();
    }

    static SnapshotImage decode(byte[] bytes, int maxBytes, int maxEntries, int maxValueBytes) throws IOException {
        if (bytes.length > maxBytes) throw new IOException("Snapshot exceeds configured byte limit");
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        if (in.readInt() != MAGIC) throw new IOException("Unknown snapshot format");
        String group = readText(in, 512);
        if (in.readInt() != 3) throw new IOException("Invalid snapshot membership");
        List<String> members = List.of(readText(in, 512), readText(in, 512), readText(in, 512));
        long index = in.readLong(), term = in.readLong();
        if (index <= 0 || term <= 0) throw new IOException("Invalid snapshot boundary");
        Map<String, byte[]> values = new HashMap<>();
        int keys = count(in, maxEntries);
        for (int i = 0; i < keys; i++) {
            String key = readText(in, 4096);
            byte[] value = readBytes(in, maxValueBytes);
            if (key.isBlank() || key.length() > 1024 || values.put(key, value) != null) {
                throw new IOException("Invalid or duplicate snapshot key");
            }
        }
        Map<String, SnapshotImage.AppliedRequest> requests = new HashMap<>();
        int requestCount = count(in, maxEntries);
        for (int i = 0; i < requestCount; i++) {
            String id = readText(in, 512);
            byte[] digest = in.readNBytes(32);
            if (digest.length != 32) throw new EOFException("Truncated command fingerprint");
            String fingerprint = HexFormat.of().formatHex(digest);
            boolean found = in.readBoolean(); long originalIndex = in.readLong();
            if (id.isBlank() || id.length() > 128 || !fingerprint.matches("[0-9a-f]{64}")
                    || originalIndex <= 0 || originalIndex > index
                    || requests.put(id, new SnapshotImage.AppliedRequest(fingerprint,
                    new CommandResult(found, null, originalIndex))) != null) {
                throw new IOException("Invalid snapshot request history");
            }
        }
        if (in.available() != 0) throw new IOException("Trailing snapshot bytes");
        return new SnapshotImage(group, members, index, term, values, requests);
    }

    static String fingerprint(Command command) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update((byte) command.type().ordinal());
            String key = command.key();
            digest.update(java.nio.ByteBuffer.allocate(4).putInt(key.length()).array());
            for (int i = 0; i < key.length(); i++) {
                digest.update((byte) (key.charAt(i) >>> 8)); digest.update((byte) key.charAt(i));
            }
            byte[] value = command.value();
            digest.update(java.nio.ByteBuffer.allocate(4).putInt(value == null ? -1 : value.length).array());
            if (value != null) digest.update(value);
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    static String checksum(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    static int textBytes(String value) { return 4 + value.length() * 2; }
    static long valueBytes(String key, byte[] value) { return textBytes(key) + 4L + value.length; }
    static long requestBytes(String id) { return textBytes(id) + 32L + 1 + 8; }
    static long headerBytes(String group, List<String> members) {
        return 4L + textBytes(group) + 4 + members.stream().mapToLong(SnapshotCodec::textBytes).sum() + 16 + 8;
    }

    private static void writeText(DataOutputStream out, String value) throws IOException {
        // Preserve Java UTF-16 code units exactly, including keys containing surrogate code units.
        out.writeInt(value.length() * 2); out.writeChars(value);
    }
    private static String readText(DataInputStream in, int max) throws IOException {
        byte[] bytes = readBytes(in, max);
        if ((bytes.length & 1) != 0) throw new IOException("Invalid snapshot text length");
        char[] chars = new char[bytes.length / 2];
        for (int i = 0; i < chars.length; i++) chars[i] = (char) ((bytes[2 * i] & 255) << 8 | bytes[2 * i + 1] & 255);
        return new String(chars);
    }
    private static byte[] readBytes(DataInputStream in, int max) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > max || size > in.available()) throw new IOException("Invalid snapshot field length");
        return in.readNBytes(size);
    }
    private static int count(DataInputStream in, int max) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > max || count > in.available()) throw new IOException("Invalid snapshot entry count");
        return count;
    }
}
