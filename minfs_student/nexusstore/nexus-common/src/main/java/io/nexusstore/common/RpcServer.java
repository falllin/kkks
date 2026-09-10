package io.nexusstore.common;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Handlers must return promptly; blocking storage work belongs on the Raft executor. */
public final class RpcServer implements AutoCloseable {
    private final String host;
    private final int configuredPort;
    private final Function<RpcRequest, CompletableFuture<RpcResponse>> handler;
    private final NioEventLoopGroup acceptors;
    private final NioEventLoopGroup workers;
    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final Semaphore capacity = new Semaphore(1024);
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Channel server;

    public RpcServer(String host, int port, Function<RpcRequest, CompletableFuture<RpcResponse>> handler) {
        this.host = Objects.requireNonNull(host, "host");
        if (port < 0 || port > 65535) throw new IllegalArgumentException("port outside 0..65535");
        this.configuredPort = port;
        this.handler = Objects.requireNonNull(handler, "handler");
        acceptors = new NioEventLoopGroup(1);
        workers = new NioEventLoopGroup(2);
    }

    /** Call from the application startup thread. Binding is the only synchronous network operation. */
    public synchronized void start() throws InterruptedException {
        if (closed.get()) throw new IllegalStateException("RPC server closed");
        if (server != null) return;
        for (var eventLoop : workers) {
            if (eventLoop.inEventLoop()) throw new IllegalStateException("Do not start server on its event loop");
        }
        server = new ServerBootstrap().group(acceptors, workers).channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(2 * 1024 * 1024, 4 * 1024 * 1024))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel channel) {
                        channels.add(channel);
                        RpcFrames.install(channel.pipeline());
                        channel.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            private final Semaphore perConnection = new Semaphore(128);
                            private final java.util.Set<Runnable> releases = ConcurrentHashMap.newKeySet();

                            @Override protected void channelRead0(ChannelHandlerContext context, ByteBuf frame) throws Exception {
                                RpcRequest request = RpcFrames.decode(frame, RpcRequest.class);
                                if (request.method() == null || request.group() == null) {
                                    respond(context, RpcResponse.fail(request.id(), "method and group required", null));
                                    return;
                                }
                                if (!perConnection.tryAcquire()) {
                                    respond(context, RpcResponse.fail(request.id(), "RPC connection overloaded", null));
                                    return;
                                }
                                if (!capacity.tryAcquire()) {
                                    perConnection.release();
                                    respond(context, RpcResponse.fail(request.id(), "RPC server overloaded", null));
                                    return;
                                }
                                AtomicBoolean released = new AtomicBoolean();
                                Runnable release = () -> {
                                    if (released.compareAndSet(false, true)) {
                                        capacity.release();
                                        perConnection.release();
                                    }
                                };
                                releases.add(release);
                                Runnable cleanup = () -> {
                                    release.run();
                                    releases.remove(release);
                                };
                                CompletableFuture<RpcResponse> response;
                                try {
                                    response = Objects.requireNonNull(handler.apply(request), "handler future");
                                } catch (Exception error) {
                                    response = CompletableFuture.failedFuture(error);
                                }
                                response.whenComplete((result, error) -> {
                                    if (error == null && result != null) respond(context, result, cleanup);
                                    else {
                                        Throwable cause = error;
                                        while (cause != null && cause.getCause() != null) cause = cause.getCause();
                                        String leaderId = cause instanceof RpcException rpc ? rpc.leaderId() : null;
                                        respond(context, RpcResponse.fail(request.id(), cause == null ? "Null handler response" : cause.getMessage(), leaderId), cleanup);
                                    }
                                });
                            }

                            @Override public void channelInactive(ChannelHandlerContext context) {
                                releases.forEach(Runnable::run);
                                releases.clear();
                            }

                            @Override public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
                                context.close();
                            }
                        });
                    }
                }).bind(host, configuredPort).sync().channel();
        channels.add(server);
    }

    private void respond(ChannelHandlerContext context, RpcResponse response) {
        respond(context, response, () -> { });
    }

    private void respond(ChannelHandlerContext context, RpcResponse response, Runnable cleanup) {
        Runnable write = () -> {
            try {
                if (!context.channel().isActive()) return;
                if (!context.channel().isWritable()) {
                    context.close();
                    return;
                }
                context.writeAndFlush(RpcFrames.encode(response)).addListener(future -> {
                    if (!future.isSuccess()) context.close();
                });
            } catch (Exception error) {
                context.close();
            } finally {
                cleanup.run();
            }
        };
        // Immediate responses must not create an unbounded executor queue while a read batch is decoded.
        if (context.executor().inEventLoop()) write.run();
        else {
            try {
                context.executor().execute(write);
            } catch (RejectedExecutionException shutdown) {
                cleanup.run();
            }
        }
    }

    public int port() {
        Channel bound = server;
        return bound == null ? configuredPort : ((InetSocketAddress) bound.localAddress()).getPort();
    }

    @Override public synchronized void close() {
        if (!closed.compareAndSet(false, true)) return;
        channels.close();
        acceptors.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        workers.shutdownGracefully(0, 2, TimeUnit.SECONDS);
    }
}
