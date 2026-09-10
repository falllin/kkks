package io.nexusstore.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

/** Wire format: unsigned 4-byte big-endian JSON length followed by UTF-8 JSON. */
final class RpcFrames {
    static final int MAX_BYTES = 8 * 1024 * 1024;

    private RpcFrames() { }

    static void install(ChannelPipeline pipeline) {
        pipeline.addLast(new LengthFieldBasedFrameDecoder(MAX_BYTES + 4, 0, 4, 0, 4));
        pipeline.addLast(new LengthFieldPrepender(4));
    }

    static ByteBuf encode(Object value) throws java.io.IOException {
        byte[] bytes = Json.MAPPER.writeValueAsBytes(value);
        if (bytes.length > MAX_BYTES) {
            throw new java.io.IOException("RPC frame exceeds 8 MiB");
        }
        return Unpooled.wrappedBuffer(bytes);
    }

    static <T> T decode(ByteBuf frame, Class<T> type) throws java.io.IOException {
        byte[] bytes = new byte[frame.readableBytes()];
        frame.readBytes(bytes);
        return Json.MAPPER.readValue(bytes, type);
    }
}
