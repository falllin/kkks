package io.nexusstore.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.JsonGenerator;
import io.nexusstore.raft.RaftMessages.*;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import io.nexusstore.raft.SnapshotImage.AppliedRequest;

/**
 * Small, fixed three-member Raft implementation for interviews and fault-injection demos.
 * All mutable consensus state belongs to one event loop. Transport callbacks only enqueue work.
 * Persistence rewrites one bounded JSON state file, forces its bytes, then atomically replaces it.
 * This favors explainability over throughput. No membership changes or
 * power-loss durability guarantee (the containing directory is not fsynced on Windows).
 */
public final class RaftNode implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(RaftNode.class.getName());
    private static final ObjectMapper JSON = new ObjectMapper(com.fasterxml.jackson.core.JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder().maxStringLength(96 * 1024 * 1024).build()).build());
    private static final int FORMAT_VERSION = 2;
    private static final int SNAPSHOT_CHUNK_BYTES = 256 * 1024;
    private final String nodeId;
    private final String groupId;
    private final List<String> peers;
    private final List<String> members;
    private final Path stateFile;
    private final FileChannel lockChannel;
    private final FileLock directoryLock;
    private final RaftTransport transport;
    private final ScheduledExecutorService loop;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Thread eventThread;
    private volatile NodeStatus snapshot;
    private volatile Throwable failure;

    private final int maxPending = positiveInt("nexus.raft.maxPending", 128);
    private final int maxLogEntries = positiveInt("nexus.raft.maxLogEntries", 20_000);
    private final int maxCommandBytes = positiveInt("nexus.raft.maxCommandBytes", 1024 * 1024);
    private final long maxLogBytes = positiveInt("nexus.raft.maxLogBytes", 64 * 1024 * 1024);
    private final int snapshotEntries = positiveInt("nexus.raft.snapshotEntries", 128);
    private final long snapshotLogBytes = positiveInt("nexus.raft.snapshotLogBytes", 4 * 1024 * 1024);
    private final int maxSnapshotBytes = positiveInt("nexus.raft.maxSnapshotBytes", 64 * 1024 * 1024);
    private final int maxStateEntries = positiveInt("nexus.raft.maxStateEntries", 100_000);
    private final long electionMinMs = positiveInt("nexus.raft.electionMinMs", 600);
    private final long electionMaxMs = positiveInt("nexus.raft.electionMaxMs", 1000);
    private final long heartbeatMs = positiveInt("nexus.raft.heartbeatMs", 120);
    private final long submitTimeoutMs = positiveInt("nexus.raft.submitTimeoutMs", 2500);
    private final long rpcTimeoutMs = positiveInt("nexus.raft.rpcTimeoutMs", 500);
    private final Semaphore submissionSlots = new Semaphore(maxPending);
    private final Semaphore inboundSlots = new Semaphore(256);

    private String role = "FOLLOWER";
    private long term;
    private String votedFor;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private long logBytes;
    private SnapshotRecord storedSnapshot;
    private IncomingSnapshot incomingSnapshot;
    private long electionDeadline;
    private long lastHeartbeat;
    private final List<LogEntry> log = new ArrayList<>();
    private final Map<String, byte[]> values = new HashMap<>();
    private final Map<String, AppliedRequest> appliedRequests = new HashMap<>();
    private final Map<Long, Pending> pending = new LinkedHashMap<>();
    private final Map<String, Pending> activeRequests = new HashMap<>();
    private final Map<String, PeerProgress> progress = new HashMap<>();
    private final Set<String> votes = new HashSet<>();

    public RaftNode(String nodeId, String groupId, List<String> peerIds,
                    Path directory, RaftTransport transport) throws IOException {
        this.nodeId = identifier(nodeId, "nodeId");
        this.groupId = identifier(groupId, "groupId");
        Objects.requireNonNull(peerIds, "peerIds");
        Set<String> unique = new TreeSet<>();
        for (String peer : peerIds) {
            if (!unique.add(identifier(peer, "peerId"))) {
                throw new IllegalArgumentException("Duplicate member: " + peer);
            }
        }
        unique.add(nodeId);
        if (unique.size() != 3) throw new IllegalArgumentException("Exactly three fixed members are required");
        this.members = List.copyOf(unique);
        this.peers = members.stream().filter(id -> !id.equals(nodeId)).toList();
        this.transport = Objects.requireNonNull(transport, "transport");
        if (maxSnapshotBytes > 64 * 1024 * 1024) throw new IllegalArgumentException("Snapshots are limited to 64 MiB");
        if (electionMinMs <= heartbeatMs * 2 || electionMaxMs <= electionMinMs) {
            throw new IllegalArgumentException("Election interval must exceed two heartbeats and have random jitter");
        }
        Files.createDirectories(Objects.requireNonNull(directory, "directory"));
        this.stateFile = directory.resolve("raft-state.json");
        this.lockChannel = FileChannel.open(directory.resolve("raft.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock acquired = null;
        try {
            acquired = lockChannel.tryLock();
            if (acquired == null) throw new IOException("Raft data directory is already in use: " + directory);
            this.directoryLock = acquired;
            load();
        } catch (IOException | RuntimeException e) {
            if (acquired != null) acquired.close();
            lockChannel.close();
            throw e;
        }
        this.loop = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "raft-" + groupId + "-" + nodeId);
            thread.setDaemon(true);
            eventThread = thread;
            return thread;
        });
        refreshStatus();
    }

    public void start() {
        if (closed.get()) throw new IllegalStateException("Raft node is closed");
        if (started.compareAndSet(false, true)) {
            loop.execute(() -> {
                resetElectionDeadline();
                loop.scheduleWithFixedDelay(() -> internal(this::tick), 20, 20, TimeUnit.MILLISECONDS);
            });
        }
    }

    /** A timeout has an UNKNOWN outcome: an uncommitted operation may commit after connectivity returns. */
    public CompletableFuture<CommandResult> submit(Command command) {
        CompletableFuture<CommandResult> result = new CompletableFuture<>();
        if (!submissionSlots.tryAcquire()) {
            return CompletableFuture.failedFuture(new RejectedExecutionException("Too many outstanding Raft submissions"));
        }
        result.whenComplete((ignored, error) -> submissionSlots.release());
        dispatch(result, () -> {
            validateCommand(command, false);
            if (!role.equals("LEADER")) throw new NotLeaderException(leaderId);
            if (isMutation(command)) {
                AppliedRequest applied = appliedRequests.get(command.requestId());
                if (applied != null && !applied.fingerprint().equals(SnapshotCodec.fingerprint(command))) {
                    throw new IllegalArgumentException("requestId was already used for a different command");
                }
                // Include uncommitted entries: an older command may be committed by our initial NOOP.
                for (LogEntry entry : log) {
                    Command previous = entry.command();
                    if (isMutation(previous) && previous.requestId().equals(command.requestId())
                            && !sameCommand(previous, command)) {
                        throw new IllegalArgumentException("requestId was already used for a different command");
                    }
                }
                Pending existing = activeRequests.get(command.requestId());
                if (existing != null) {
                    existing.future.whenComplete((value, error) -> {
                        if (error == null) result.complete(value); else result.completeExceptionally(error);
                    });
                    return;
                }
            }
            if (pending.size() >= maxPending) throw new RejectedExecutionException("Too many pending Raft commands");
            ensureProjectedCapacity(command);
            LogEntry entry = appendLocal(command);
            Pending submission = new Pending(command, result,
                    System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(submitTimeoutMs));
            pending.put(entry.index(), submission);
            if (isMutation(command)) activeRequests.put(command.requestId(), submission);
            replicateAll();
        });
        return result;
    }

    public CompletableFuture<VoteResponse> onRequestVote(VoteRequest request) {
        CompletableFuture<VoteResponse> result = new CompletableFuture<>();
        dispatch(result, () -> result.complete(handleVote(request)));
        return result;
    }

    public CompletableFuture<AppendResponse> onAppendEntries(AppendRequest request) {
        CompletableFuture<AppendResponse> result = new CompletableFuture<>();
        dispatch(result, () -> result.complete(handleAppend(request)));
        return result;
    }

    public CompletableFuture<InstallSnapshotResponse> onInstallSnapshot(InstallSnapshotRequest request) {
        CompletableFuture<InstallSnapshotResponse> result = new CompletableFuture<>();
        dispatch(result, () -> result.complete(handleInstallSnapshot(request)));
        return result;
    }

    /** Compact only this node's applied prefix; safe on a follower as well as a leader. */
    public CompletableFuture<NodeStatus> compact() {
        CompletableFuture<NodeStatus> result = new CompletableFuture<>();
        dispatch(result, () -> { compactApplied(); refreshStatus(); result.complete(snapshot); });
        return result;
    }

    public NodeStatus status() { return snapshot; }

    private VoteResponse handleVote(VoteRequest request) {
        if (request == null || !peers.contains(request.candidateId()) || request.term() <= 0
                || request.lastLogIndex() < 0 || request.lastLogTerm() < 0
                || request.lastLogTerm() > request.term()
                || (request.lastLogIndex() == 0) != (request.lastLogTerm() == 0)) {
            throw new IllegalArgumentException("Invalid vote request or non-member candidate");
        }
        if (request.term() > term) becomeFollower(request.term(), null);
        if (request.term() < term) return new VoteResponse(term, false);
        boolean upToDate = request.lastLogTerm() > termAt(lastLogIndex())
                || (request.lastLogTerm() == termAt(lastLogIndex()) && request.lastLogIndex() >= lastLogIndex());
        boolean granted = upToDate && (votedFor == null || votedFor.equals(request.candidateId()));
        if (granted) {
            votedFor = request.candidateId();
            resetElectionDeadline();
            persist(); // Never acknowledge a vote which a process restart could forget.
        }
        return new VoteResponse(term, granted);
    }

    private AppendResponse handleAppend(AppendRequest request) {
        validateAppend(request);
        if (request.term() < term) return rejected(lastLogIndex() + 1);
        if (request.term() > term || !role.equals("FOLLOWER")) {
            becomeFollower(request.term(), request.leaderId());
        }
        leaderId = request.leaderId();
        resetElectionDeadline();
        if (request.prevLogIndex() < snapshotIndex()) return rejected(snapshotIndex() + 1);
        if (request.prevLogIndex() > lastLogIndex()) return rejected(lastLogIndex() + 1);
        if (termAt(request.prevLogIndex()) != request.prevLogTerm()) {
            long conflict = request.prevLogIndex();
            long conflictTerm = termAt(conflict);
            while (conflict > snapshotIndex() + 1 && termAt(conflict - 1) == conflictTerm) conflict--;
            return rejected(conflict);
        }
        int replaceFrom = -1;
        for (int i = 0; i < request.entries().size(); i++) {
            LogEntry incoming = request.entries().get(i);
            if (incoming.index() > lastLogIndex() || termAt(incoming.index()) != incoming.term()) {
                if (incoming.index() <= commitIndex) return rejected(commitIndex + 1);
                replaceFrom = i;
                break;
            }
            if (!sameCommand(entryAt(incoming.index()).command(), incoming.command())) {
                throw new IllegalArgumentException("Same log index/term contains different commands");
            }
        }
        boolean changed = false;
        if (replaceFrom >= 0) {
            int keep = offset(request.entries().get(replaceFrom).index());
            long bytes = replacementBytes(keep, request.entries(), replaceFrom);
            if (keep + request.entries().size() - replaceFrom > maxLogEntries || bytes > maxLogBytes) {
                // This RPC already verified the existing prefix before its first new/conflicting entry.
                // Durably commit that prefix before reclaiming it: a full follower must still catch up
                // when the same RPC brings both a new entry and the leader's newer commit index.
                long verifiedExisting = request.entries().get(replaceFrom).index() - 1;
                long reclaimCommit = Math.min(request.leaderCommit(), verifiedExisting);
                if (reclaimCommit > commitIndex) {
                    commitIndex = reclaimCommit;
                    persist();
                    applyCommitted();
                }
                compactApplied();
                // Applying may have auto-compacted, so all physical offsets must be recomputed.
                keep = offset(request.entries().get(replaceFrom).index());
                bytes = replacementBytes(keep, request.entries(), replaceFrom);
            }
            ensureCapacity(keep + request.entries().size() - replaceFrom, bytes);
            List<LogEntry> prospective = new ArrayList<>(log.subList(0, keep));
            prospective.addAll(request.entries().subList(replaceFrom, request.entries().size()));
            ensureProjectedCapacity(prospective, null);
            log.subList(keep, log.size()).clear();
            log.addAll(request.entries().subList(replaceFrom, request.entries().size()));
            logBytes = bytes;
            changed = true;
        }
        long matched = request.prevLogIndex() + request.entries().size();
        // Batch replication may leave an unverified divergent tail: do not commit that tail.
        long newCommit = Math.min(request.leaderCommit(), matched);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            changed = true;
        }
        if (changed) persist(); // A positive AppendEntries response means entries are durable.
        applyCommitted();
        return new AppendResponse(term, true, matched, 0);
    }

    private long replacementBytes(int keep, List<LogEntry> incoming, int replaceFrom) {
        long bytes = logBytes;
        for (int i = keep; i < log.size(); i++) bytes -= entryBytes(log.get(i));
        for (int i = replaceFrom; i < incoming.size(); i++) bytes += entryBytes(incoming.get(i));
        return bytes;
    }

    private void tick() {
        long now = System.nanoTime();
        if (role.equals("LEADER")) {
            if (now - lastHeartbeat >= TimeUnit.MILLISECONDS.toNanos(heartbeatMs)) {
                lastHeartbeat = now;
                replicateAll();
            }
            Iterator<Map.Entry<Long, Pending>> iterator = pending.entrySet().iterator();
            while (iterator.hasNext()) {
                Pending item = iterator.next().getValue();
                if (now >= item.deadline) {
                    iterator.remove();
                    removeActive(item);
                    item.future.completeExceptionally(new TimeoutException(
                            "Majority did not commit within " + submitTimeoutMs + " ms; outcome is unknown; retry with the same requestId"));
                }
            }
        } else if (now >= electionDeadline) {
            beginElection();
        }
    }

    private void beginElection() {
        role = "CANDIDATE";
        leaderId = null;
        incomingSnapshot = null;
        term = Math.addExact(term, 1);
        votedFor = nodeId;
        votes.clear();
        votes.add(nodeId);
        resetElectionDeadline();
        persist();
        long electionTerm = term;
        VoteRequest request = new VoteRequest(term, nodeId, lastLogIndex(), termAt(lastLogIndex()));
        for (String peer : peers) {
            try {
                transport.requestVote(peer, groupId, request).orTimeout(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                        .whenComplete((response, error) -> callback(() -> {
                            if (error != null || response == null) return;
                            if (response.term() > term) { becomeFollower(response.term(), null); return; }
                            if (!role.equals("CANDIDATE") || term != electionTerm || response.term() != term) return;
                            if (response.voteGranted()) votes.add(peer);
                            if (votes.size() >= 2) becomeLeader();
                        }));
            } catch (RuntimeException ignored) {
                // A disconnected transport is an unavailable voter, never an implicit vote.
            }
        }
    }

    private void becomeLeader() {
        role = "LEADER";
        leaderId = nodeId;
        progress.clear();
        for (String peer : peers) progress.put(peer, new PeerProgress(lastLogIndex() + 1));
        appendLocal(new Command(Command.Type.NOOP, "", null, ""));
        lastHeartbeat = System.nanoTime();
        replicateAll();
    }

    private void becomeFollower(long newTerm, String newLeader) {
        if (newTerm < term) throw new IllegalArgumentException("Term cannot decrease");
        boolean termChanged = newTerm > term;
        term = newTerm;
        if (termChanged) votedFor = null;
        if (termChanged || (incomingSnapshot != null && !Objects.equals(newLeader, incomingSnapshot.leaderId))) {
            incomingSnapshot = null;
        }
        role = "FOLLOWER";
        leaderId = newLeader;
        votes.clear();
        progress.clear();
        resetElectionDeadline();
        failPending(new NotLeaderException(newLeader));
        if (termChanged) persist();
    }

    private LogEntry appendLocal(Command command) {
        // Reclaim an applied prefix before refusing a bounded retained tail.
        if (lastApplied > snapshotIndex() && (log.size() >= maxLogEntries
                || logBytes + entryBytes(new LogEntry(0, term, command)) > maxLogBytes)) compactApplied();
        LogEntry entry = new LogEntry(Math.addExact(lastLogIndex(), 1), term, command);
        long newBytes = logBytes + entryBytes(entry);
        ensureCapacity(log.size() + 1, newBytes);
        log.add(entry);
        logBytes = newBytes;
        persist();
        return entry;
    }

    private void replicateAll() {
        for (String peer : peers) replicate(peer);
    }

    private void replicate(String peer) {
        PeerProgress state = progress.get(peer);
        if (state == null || state.inFlight || !role.equals("LEADER")) return;
        if (state.nextIndex <= snapshotIndex()) { replicateSnapshot(peer, state); return; }
        long previous = state.nextIndex - 1;
        List<LogEntry> entries = new ArrayList<>();
        long bytes = 0;
        for (int i = offset(previous + 1); i < log.size() && entries.size() < 16; i++) {
            LogEntry entry = log.get(i);
            long size = entryBytes(entry);
            if (!entries.isEmpty() && bytes + size > 1024 * 1024) break;
            entries.add(entry);
            bytes += size;
        }
        long sentTerm = term;
        AppendRequest request = new AppendRequest(term, nodeId, previous, termAt(previous), entries, commitIndex);
        state.inFlight = true;
        try {
            transport.appendEntries(peer, groupId, request).orTimeout(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                    .whenComplete((response, error) -> callback(() -> {
                        if (response != null && response.term() > term) {
                            becomeFollower(response.term(), null);
                            return;
                        }
                        if (!role.equals("LEADER") || sentTerm != term || progress.get(peer) != state) return;
                        state.inFlight = false;
                        if (error != null || response == null || response.term() != term) return;
                        if (response.success()) {
                            long expectedMatch = request.prevLogIndex() + request.entries().size();
                            if (response.matchIndex() != expectedMatch) return;
                            state.matchIndex = Math.max(state.matchIndex, expectedMatch);
                            state.nextIndex = state.matchIndex + 1;
                            advanceCommit();
                            if (state.nextIndex <= lastLogIndex()) replicate(peer);
                        } else {
                            long hint = response.conflictIndex();
                            if (hint > request.prevLogIndex() + 1) {
                                // A follower may have compacted farther than this peer's last acknowledged index.
                                // Probe its boundary; never infer matchIndex from this rejection alone.
                                state.nextIndex = Math.min(lastLogIndex() + 1, hint);
                            } else {
                                state.nextIndex = Math.max(1, Math.min(state.nextIndex - 1, hint));
                            }
                            replicate(peer);
                        }
                    }));
        } catch (RuntimeException ignored) {
            state.inFlight = false;
        }
    }

    private void advanceCommit() {
        long[] matches = { lastLogIndex(), progress.get(peers.get(0)).matchIndex, progress.get(peers.get(1)).matchIndex };
        Arrays.sort(matches);
        long majorityIndex = matches[1];
        // Never commit an old-term entry by counting replicas; a current-term NOOP establishes safety.
        if (majorityIndex > commitIndex && termAt(majorityIndex) == term) {
            commitIndex = majorityIndex;
            persist();
            applyCommitted();
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            LogEntry entry = entryAt(lastApplied + 1);
            Command command = entry.command();
            CommandResult result;
            AppliedRequest previous = isMutation(command) ? appliedRequests.get(command.requestId()) : null;
            if (previous != null) {
                if (!previous.fingerprint().equals(SnapshotCodec.fingerprint(command))) throw new IllegalStateException("Conflicting committed requestId");
                result = previous.result();
            } else {
                result = switch (command.type()) {
                    case PUT -> {
                        values.put(command.key(), command.value());
                        yield new CommandResult(true, null, entry.index());
                    }
                    case GET -> {
                        byte[] value = values.get(command.key());
                        yield new CommandResult(value != null, value, entry.index());
                    }
                    case DELETE -> new CommandResult(values.remove(command.key()) != null, null, entry.index());
                    case NOOP -> new CommandResult(false, null, entry.index());
                };
                if (isMutation(command)) appliedRequests.put(command.requestId(), new AppliedRequest(SnapshotCodec.fingerprint(command), result));
            }
            lastApplied = entry.index();
            Pending submission = pending.remove(entry.index());
            if (submission != null) {
                removeActive(submission);
                submission.future.complete(result);
            }
        }
        if (incomingSnapshot != null && incomingSnapshot.index <= commitIndex) incomingSnapshot = null;
        if (lastApplied > snapshotIndex() && (lastApplied - snapshotIndex() >= snapshotEntries
                || appliedLogBytes() >= snapshotLogBytes)) compactApplied();
    }

    private long appliedLogBytes() {
        long bytes = 0;
        for (LogEntry entry : log) {
            if (entry.index() > lastApplied) break;
            bytes += entryBytes(entry);
        }
        return bytes;
    }

    private void compactApplied() {
        if (lastApplied <= snapshotIndex()) return;
        long boundary = lastApplied;
        long boundaryTerm = termAt(boundary);
        final byte[] bytes;
        try {
            bytes = SnapshotCodec.encode(new SnapshotImage(groupId, members, boundary, boundaryTerm,
                    values, appliedRequests), maxSnapshotBytes);
        } catch (IOException error) {
            throw new IllegalStateException("Cannot create bounded snapshot", error);
        }
        int remove = Math.toIntExact(boundary - snapshotIndex());
        log.subList(0, remove).clear();
        logBytes = log.stream().mapToLong(this::entryBytes).sum();
        storedSnapshot = new SnapshotRecord(boundary, boundaryTerm, SnapshotCodec.checksum(bytes), bytes);
        // The snapshot and remaining tail become durable together in one atomic file replacement.
        persist();
    }

    private void restoreImage(SnapshotImage image) {
        values.clear(); values.putAll(image.values());
        appliedRequests.clear(); appliedRequests.putAll(image.requests());
        lastApplied = image.index();
    }

    private void validateImage(SnapshotImage image, long index, long includedTerm) throws IOException {
        if (!groupId.equals(image.groupId()) || !members.equals(image.members())
                || image.index() != index || image.term() != includedTerm) {
            throw new IOException("Snapshot identity, membership or boundary does not match");
        }
    }

    /** Include all outstanding entries: concurrent writes cannot each spend the same free space. */
    private void ensureProjectedCapacity(Command proposed) {
        if (!isMutation(proposed)) return;
        ensureProjectedCapacity(log, proposed);
    }

    private void ensureProjectedCapacity(List<LogEntry> prospective, Command proposed) {
        Map<String, Integer> sizes = new HashMap<>();
        long bytes = SnapshotCodec.headerBytes(groupId, members);
        for (Map.Entry<String, byte[]> item : values.entrySet()) {
            sizes.put(item.getKey(), item.getValue().length);
            bytes += SnapshotCodec.valueBytes(item.getKey(), item.getValue());
        }
        Set<String> ids = new HashSet<>(appliedRequests.keySet());
        for (String id : ids) bytes += SnapshotCodec.requestBytes(id);
        List<Command> commands = new ArrayList<>();
        for (LogEntry entry : prospective) if (entry.index() > lastApplied) commands.add(entry.command());
        if (proposed != null) commands.add(proposed);
        for (Command command : commands) {
            if (!isMutation(command) || !ids.add(command.requestId())) continue;
            bytes += SnapshotCodec.requestBytes(command.requestId());
            Integer oldSize = sizes.remove(command.key());
            if (oldSize != null) bytes -= SnapshotCodec.textBytes(command.key()) + 4L + oldSize;
            if (command.type() == Command.Type.PUT) {
                int length = command.value().length;
                sizes.put(command.key(), length);
                bytes += SnapshotCodec.textBytes(command.key()) + 4L + length;
            }
            if (bytes > maxSnapshotBytes || sizes.size() > maxStateEntries || ids.size() > maxStateEntries) {
                throw new RejectedExecutionException("State-machine capacity reached (live values plus permanent mutation history)");
            }
        }
    }

    private InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        validateSnapshotRequest(request);
        if (request.term() < term) return new InstallSnapshotResponse(term, false, 0, 0);
        if (request.term() > term || !role.equals("FOLLOWER")) becomeFollower(request.term(), request.leaderId());
        leaderId = request.leaderId();
        resetElectionDeadline();
        // Already committed state never rolls back. It is also proof that this prefix is durable.
        if (request.lastIncludedIndex() <= commitIndex) {
            if (incomingSnapshot != null && incomingSnapshot.index <= commitIndex) incomingSnapshot = null;
            return new InstallSnapshotResponse(term, true, request.totalSize(), request.lastIncludedIndex());
        }
        if (incomingSnapshot == null || !incomingSnapshot.matches(request)) {
            if (request.offset() != 0) return new InstallSnapshotResponse(term, false, 0, 0);
            incomingSnapshot = new IncomingSnapshot(request);
        }
        IncomingSnapshot receiving = incomingSnapshot;
        byte[] chunk = request.data();
        if (request.offset() > receiving.received) {
            return new InstallSnapshotResponse(term, false, receiving.received, 0);
        }
        int offset = Math.toIntExact(request.offset());
        if (offset < receiving.received) {
            if (offset + chunk.length > receiving.received
                    || !Arrays.equals(receiving.bytes, offset, offset + chunk.length, chunk, 0, chunk.length)) {
                return new InstallSnapshotResponse(term, false, receiving.received, 0);
            }
            return new InstallSnapshotResponse(term, true, receiving.received, 0);
        }
        System.arraycopy(chunk, 0, receiving.bytes, offset, chunk.length);
        receiving.received += chunk.length;
        if (!request.done()) return new InstallSnapshotResponse(term, true, receiving.received, 0);
        if (!receiving.checksum.equals(SnapshotCodec.checksum(receiving.bytes))) {
            incomingSnapshot = null;
            throw new IllegalArgumentException("Snapshot checksum mismatch");
        }
        final SnapshotImage image;
        try {
            image = SnapshotCodec.decode(receiving.bytes, maxSnapshotBytes, maxStateEntries, maxCommandBytes);
            validateImage(image, receiving.index, receiving.snapshotTerm);
        } catch (IOException error) {
            incomingSnapshot = null;
            throw new IllegalArgumentException("Invalid snapshot image", error);
        }
        // Retain only a suffix whose boundary term matches the leader's snapshot.
        boolean matching = receiving.index <= lastLogIndex() && termAt(receiving.index) == receiving.snapshotTerm;
        if (matching) log.subList(0, Math.toIntExact(receiving.index - snapshotIndex())).clear();
        else log.clear();
        logBytes = log.stream().mapToLong(this::entryBytes).sum();
        storedSnapshot = new SnapshotRecord(receiving.index, receiving.snapshotTerm,
                receiving.checksum, receiving.bytes);
        restoreImage(image);
        commitIndex = receiving.index;
        incomingSnapshot = null;
        persist();
        return new InstallSnapshotResponse(term, true, request.totalSize(), receiving.index);
    }

    private void validateSnapshotRequest(InstallSnapshotRequest request) {
        byte[] data = request == null ? null : request.data();
        if (request == null || !peers.contains(request.leaderId()) || request.term() <= 0
                || request.snapshotId() == null || request.snapshotId().isBlank() || request.snapshotId().length() > 128
                || request.lastIncludedIndex() <= 0 || request.lastIncludedIndex() == Long.MAX_VALUE
                || request.lastIncludedTerm() <= 0 || request.lastIncludedTerm() > request.term()
                || request.totalSize() <= 0 || request.totalSize() > maxSnapshotBytes
                || request.offset() < 0 || request.offset() >= request.totalSize()
                || data == null || data.length == 0 || data.length > SNAPSHOT_CHUNK_BYTES
                || data.length > request.totalSize() - request.offset()
                || request.done() != (request.offset() + data.length == request.totalSize())
                || request.checksum() == null || !request.checksum().matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Invalid snapshot chunk or non-member leader");
        }
    }

    private void replicateSnapshot(String peer, PeerProgress state) {
        // Finish one immutable image even if automatic compaction produces a newer one meanwhile.
        if (state.transfer == null) { state.transfer = storedSnapshot; state.transferOffset = 0; }
        SnapshotRecord sending = state.transfer;
        int offset = Math.toIntExact(state.transferOffset);
        int end = Math.min(sending.data().length, offset + SNAPSHOT_CHUNK_BYTES);
        byte[] chunk = Arrays.copyOfRange(sending.data(), offset, end);
        long sentTerm = term;
        InstallSnapshotRequest request = new InstallSnapshotRequest(term, nodeId, sending.checksum(),
                sending.index(), sending.term(), offset, chunk, end == sending.data().length,
                sending.checksum(), sending.data().length);
        state.inFlight = true;
        try {
            transport.installSnapshot(peer, groupId, request).orTimeout(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                    .whenComplete((response, error) -> callback(() -> {
                        if (response != null && response.term() > term) { becomeFollower(response.term(), null); return; }
                        if (!role.equals("LEADER") || sentTerm != term || progress.get(peer) != state) return;
                        state.inFlight = false;
                        if (error != null || response == null || response.term() != term) return;
                        if (response.nextOffset() < 0 || response.nextOffset() > sending.data().length) return;
                        if (response.success() && response.lastIncludedIndex() == sending.index()
                                && response.nextOffset() == sending.data().length) {
                            state.matchIndex = Math.max(state.matchIndex, sending.index());
                            state.nextIndex = state.matchIndex + 1;
                            state.transfer = null;
                            state.transferOffset = 0;
                            advanceCommit();
                            replicate(peer);
                        } else if (response.nextOffset() < sending.data().length) {
                            state.transferOffset = response.nextOffset();
                            // Rejections retry on the next heartbeat, avoiding a tight failed-RPC loop.
                            if (response.success()) replicate(peer);
                        }
                    }));
        } catch (RuntimeException ignored) { state.inFlight = false; }
    }

    private void load() throws IOException {
        if (!Files.exists(stateFile)) {
            writeState();
            return;
        }
        if (Files.size(stateFile) > maxLogBytes * 2 + maxSnapshotBytes * 2L + 4L * 1024 * 1024) {
            throw new IOException("Persisted state exceeds configured bounds");
        }
        PersistentState state = JSON.readValue(stateFile.toFile(), PersistentState.class);
        if ((state.formatVersion() != 1 && state.formatVersion() != FORMAT_VERSION) || !nodeId.equals(state.nodeId())
                || !groupId.equals(state.groupId()) || !members.equals(state.members())) {
            throw new IOException("Data directory identity, format or fixed membership does not match configuration");
        }
        if (state.term() < 0 || (state.votedFor() != null && !members.contains(state.votedFor()))
                || state.log() == null || state.commitIndex() < 0
                || (state.formatVersion() == 1 && state.snapshot() != null)) {
            throw new IOException("Invalid persistent Raft state");
        }
        try {
            if (state.snapshot() != null) {
                SnapshotRecord saved = state.snapshot();
                if (saved.data() == null || saved.checksum() == null
                        || !saved.checksum().equals(SnapshotCodec.checksum(saved.data()))) {
                    throw new IOException("Snapshot checksum mismatch");
                }
                SnapshotImage image = SnapshotCodec.decode(saved.data(), maxSnapshotBytes, maxStateEntries, maxCommandBytes);
                validateImage(image, saved.index(), saved.term());
                if (saved.term() > state.term()) throw new IOException("Snapshot term exceeds current term");
                storedSnapshot = saved;
                restoreImage(image);
            }
            if (state.commitIndex() < snapshotIndex()
                    || state.commitIndex() > snapshotIndex() + state.log().size()) {
                throw new IOException("Invalid persistent commit index");
            }
            long previousTerm = snapshotTerm();
            for (LogEntry entry : state.log()) {
                if (entry == null || entry.index() != lastLogIndex() + 1 || entry.term() <= 0
                        || entry.term() < previousTerm || entry.term() > state.term()) {
                    throw new IllegalArgumentException("Invalid persistent log sequence");
                }
                validateCommand(entry.command(), true);
                log.add(entry);
                logBytes += entryBytes(entry);
                previousTerm = entry.term();
            }
            ensureCapacity(log.size(), logBytes);
            ensureProjectedCapacity(log, null);
            term = state.term();
            votedFor = state.votedFor();
            commitIndex = state.commitIndex();
            applyCommitted();
        } catch (RuntimeException invalid) {
            throw new IOException("Invalid persisted log; refusing to start", invalid);
        }
    }

    private void persist() {
        try {
            writeState();
        } catch (IOException error) {
            failClosed(error);
            throw new IllegalStateException("Raft persistence failed; node stopped acknowledging requests", error);
        }
    }

    private void writeState() throws IOException {
        PersistentState state = new PersistentState(FORMAT_VERSION, nodeId, groupId,
                members, term, votedFor, commitIndex, List.copyOf(log), storedSnapshot);
        Path temporary = stateFile.resolveSibling("raft-state.json.tmp");
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            // Stream the envelope, avoiding another snapshot+tail-sized JSON byte array in memory.
            try (JsonGenerator generator = JSON.getFactory().createGenerator(Channels.newOutputStream(channel))) {
                generator.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
                JSON.writeValue(generator, state);
            }
            channel.force(true);
        }
        // Unsupported atomic replacement is a fatal storage error; never silently degrade to an unsafe write.
        Files.move(temporary, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private void validateAppend(AppendRequest request) {
        if (request == null || !peers.contains(request.leaderId()) || request.term() <= 0
                || request.prevLogIndex() < 0 || request.prevLogIndex() > Long.MAX_VALUE - 16
                || request.prevLogTerm() < 0 || request.prevLogTerm() > request.term()
                || (request.prevLogIndex() == 0) != (request.prevLogTerm() == 0)
                || request.leaderCommit() < 0 || request.entries() == null || request.entries().size() > 16) {
            throw new IllegalArgumentException("Invalid append request or non-member leader");
        }
        long expected = request.prevLogIndex() + 1;
        long precedingTerm = request.prevLogTerm();
        long batchBytes = 0;
        for (LogEntry entry : request.entries()) {
            if (entry == null || entry.index() != expected++ || entry.term() <= 0
                    || entry.term() < precedingTerm || entry.term() > request.term()) {
                throw new IllegalArgumentException("Invalid append log sequence");
            }
            validateCommand(entry.command(), true);
            precedingTerm = entry.term();
            batchBytes += entryBytes(entry);
        }
        if (batchBytes > maxCommandBytes + 1024L * 1024 + 4096) {
            throw new IllegalArgumentException("Append batch exceeds limit");
        }
    }

    private void validateCommand(Command command, boolean allowNoop) {
        if (command == null || command.type() == null) throw new IllegalArgumentException("Command type is required");
        if (command.type() == Command.Type.NOOP) {
            if (!allowNoop || (command.key() != null && !command.key().isEmpty()) || command.value() != null
                    || (command.requestId() != null && !command.requestId().isEmpty())) {
                throw new IllegalArgumentException("NOOP is an internal command");
            }
            return;
        }
        if (command.key() == null || command.key().isBlank() || command.key().length() > 1024) {
            throw new IllegalArgumentException("Key must contain 1 to 1024 characters");
        }
        byte[] value = command.value();
        if (command.type() == Command.Type.PUT) {
            if (value == null || value.length > maxCommandBytes) throw new IllegalArgumentException("PUT value is missing or too large");
        } else if (value != null) {
            throw new IllegalArgumentException("GET/DELETE cannot contain a value");
        }
        if (isMutation(command) && (command.requestId() == null || command.requestId().isBlank()
                || command.requestId().length() > 128)) {
            throw new IllegalArgumentException("Mutation requestId must contain 1 to 128 characters");
        }
        if (command.requestId() != null && command.requestId().length() > 128) {
            throw new IllegalArgumentException("requestId is too long");
        }
    }

    private void ensureCapacity(int count, long bytes) {
        if (count > maxLogEntries || bytes > maxLogBytes) {
            throw new RejectedExecutionException("Retained Raft log capacity reached; uncommitted entries cannot be compacted");
        }
    }

    private long entryBytes(LogEntry entry) {
        Command command = entry.command();
        byte[] value = command.value();
        return 128L + (value == null ? 0 : value.length)
                + (command.key() == null ? 0 : command.key().length() * 6L)
                + (command.requestId() == null ? 0 : command.requestId().length() * 6L);
    }

    private long snapshotIndex() { return storedSnapshot == null ? 0 : storedSnapshot.index(); }
    private long snapshotTerm() { return storedSnapshot == null ? 0 : storedSnapshot.term(); }
    private long lastLogIndex() { return snapshotIndex() + log.size(); }
    private int offset(long index) { return Math.toIntExact(index - snapshotIndex() - 1); }
    private LogEntry entryAt(long index) { return log.get(offset(index)); }
    private long termAt(long index) { return index == snapshotIndex() ? snapshotTerm() : entryAt(index).term(); }
    private AppendResponse rejected(long conflictIndex) { return new AppendResponse(term, false, 0, conflictIndex); }
    private static boolean isMutation(Command command) {
        return command.type() == Command.Type.PUT || command.type() == Command.Type.DELETE;
    }
    private static boolean sameCommand(Command first, Command second) {
        return first.type() == second.type() && Objects.equals(first.key(), second.key())
                && Objects.equals(first.requestId(), second.requestId()) && Arrays.equals(first.value(), second.value());
    }
    private void resetElectionDeadline() {
        electionDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(
                ThreadLocalRandom.current().nextLong(electionMinMs, electionMaxMs));
    }
    private void removeActive(Pending item) {
        if (isMutation(item.command)) activeRequests.remove(item.command.requestId(), item);
    }
    private void failPending(Throwable error) {
        List<Pending> abandoned = new ArrayList<>(pending.values());
        pending.clear();
        activeRequests.clear();
        for (Pending item : abandoned) item.future.completeExceptionally(error);
    }
    private void failClosed(Throwable error) {
        if (failure != null || closed.get()) return;
        failure = error;
        role = "FAILED";
        leaderId = null;
        LOG.log(System.Logger.Level.ERROR, "Raft group " + groupId + " on " + nodeId
                + " failed closed; inspect storage and restart after resolving the cause", error);
        failPending(new IllegalStateException("Raft node failed closed", error));
        refreshStatus();
    }
    private void refreshStatus() {
        snapshot = new NodeStatus(nodeId, groupId, role, term, leaderId,
                commitIndex, lastApplied, lastLogIndex(), values.size(), snapshotIndex(), snapshotTerm(),
                log.size(), logBytes, storedSnapshot == null ? 0 : storedSnapshot.data().length);
    }
    private void dispatch(CompletableFuture<?> result, Runnable task) {
        if (!started.get() || closed.get()) {
            result.completeExceptionally(new IllegalStateException("Raft node is not running"));
            return;
        }
        if (!inboundSlots.tryAcquire()) {
            result.completeExceptionally(new RejectedExecutionException("Raft inbound queue is full"));
            return;
        }
        try {
            loop.execute(() -> {
                try {
                    if (failure != null || closed.get()) throw new IllegalStateException("Raft node unavailable", failure);
                    task.run();
                } catch (Throwable error) {
                    result.completeExceptionally(error);
                    if (!(error instanceof IllegalArgumentException || error instanceof NotLeaderException
                            || error instanceof RejectedExecutionException)) failClosed(error);
                } finally {
                    inboundSlots.release();
                    refreshStatus();
                }
            });
        } catch (RejectedExecutionException error) {
            inboundSlots.release();
            result.completeExceptionally(error);
        }
    }
    private void callback(Runnable task) {
        if (closed.get()) return;
        try { loop.execute(() -> internal(task)); } catch (RejectedExecutionException ignored) { }
    }
    private void internal(Runnable task) {
        if (closed.get() || failure != null) return;
        try { task.run(); } catch (Throwable error) { failClosed(error); } finally { refreshStatus(); }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        loop.execute(() -> {
            role = "CLOSED";
            leaderId = null;
            failPending(new IllegalStateException("Raft node closed"));
            refreshStatus();
            try { directoryLock.close(); lockChannel.close(); } catch (IOException error) { failure = error; }
        });
        loop.shutdown();
        if (Thread.currentThread() != eventThread) {
            try { loop.awaitTermination(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); }
        }
    }

    private static int positiveInt(String name, int fallback) {
        int value = Integer.getInteger(name, fallback);
        if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
        return value;
    }
    private static String identifier(String value, String name) {
        if (value == null || value.isBlank() || value.length() > 128) throw new IllegalArgumentException(name + " is invalid");
        return value;
    }

    public record PersistentState(int formatVersion, String nodeId, String groupId,
                                  List<String> members, long term, String votedFor,
                                  long commitIndex, List<LogEntry> log, SnapshotRecord snapshot) {
        public PersistentState(int formatVersion, String nodeId, String groupId,
                               List<String> members, long term, String votedFor,
                               long commitIndex, List<LogEntry> log) {
            this(formatVersion, nodeId, groupId, members, term, votedFor, commitIndex, log, null);
        }
    }
    public record SnapshotRecord(long index, long term, String checksum, byte[] data) { }
    private record Pending(Command command, CompletableFuture<CommandResult> future, long deadline) { }
    private static final class PeerProgress {
        long nextIndex;
        long matchIndex;
        boolean inFlight;
        SnapshotRecord transfer;
        long transferOffset;
        PeerProgress(long nextIndex) { this.nextIndex = nextIndex; }
    }
    private static final class IncomingSnapshot {
        final String leaderId;
        final long term;
        final String snapshotId;
        final long index;
        final long snapshotTerm;
        final String checksum;
        final byte[] bytes;
        int received;
        IncomingSnapshot(InstallSnapshotRequest request) {
            leaderId = request.leaderId(); term = request.term(); snapshotId = request.snapshotId();
            index = request.lastIncludedIndex(); snapshotTerm = request.lastIncludedTerm();
            checksum = request.checksum(); bytes = new byte[Math.toIntExact(request.totalSize())];
        }
        boolean matches(InstallSnapshotRequest request) {
            return term == request.term() && leaderId.equals(request.leaderId())
                    && snapshotId.equals(request.snapshotId()) && index == request.lastIncludedIndex()
                    && snapshotTerm == request.lastIncludedTerm() && checksum.equals(request.checksum())
                    && bytes.length == request.totalSize();
        }
    }
}
