package org.apache.rocketmq.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.*;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: codefans
 * @date: 2018-11-20 18:40
 */
public class RocketMQNettyServer {

    DefaultEventExecutorGroup defaultEventExecutorGroup;

    public static void main(String[] args) {
        RocketMQNettyServer rocketMQNettyServer = new RocketMQNettyServer();
        rocketMQNettyServer.startup();
    }

    public void startup() {

        try {

            int serverListenPort = 3456;
            int socketSndbufSize = 65535;
            int socketRcvbufSize = 65535;
            final int serverChannelMaxIdleTimeSeconds = 120;
            final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";

            int serverWorkerThreadCount = Runtime.getRuntime().availableProcessors();
            final int serverSelectorThreadCount = serverWorkerThreadCount;


            final DefaultEventExecutorGroup defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                    serverWorkerThreadCount,
                    new ThreadFactory() {
                        private AtomicInteger threadIndex = new AtomicInteger(0);
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
                        }
                    }
            );

            ServerBootstrap serverBootstrap = new ServerBootstrap();

            EventLoopGroup eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });

            EventLoopGroup eventLoopGroupSelector = new NioEventLoopGroup(serverSelectorThreadCount, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                private int threadTotal = serverSelectorThreadCount;
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerNIOSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });

            ServerBootstrap childHandler =
                    serverBootstrap.group(eventLoopGroupBoss, eventLoopGroupSelector)
//                            .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                            .channel(NioServerSocketChannel.class)
                            .option(ChannelOption.SO_BACKLOG, 1024)
                            .option(ChannelOption.SO_REUSEADDR, true)
                            .option(ChannelOption.SO_KEEPALIVE, false)
                            .childOption(ChannelOption.TCP_NODELAY, true)
                            .childOption(ChannelOption.SO_SNDBUF, socketSndbufSize)
                            .childOption(ChannelOption.SO_RCVBUF, socketRcvbufSize)
                            .localAddress(new InetSocketAddress(serverListenPort))
                            .childHandler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel ch) throws Exception {
                                    ch.pipeline()
                                            .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME,
                                                    new HandshakeHandler(TlsSystemConfig.tlsMode))
                                            .addLast(defaultEventExecutorGroup,
                                                    new NettyEncoder(),
                                                    new NettyDecoder(),
                                                    new IdleStateHandler(0, 0, serverChannelMaxIdleTimeSeconds),
                                                    new NettyConnectManageHandler(),
                                                    new NettyServerHandler()
                                            );
                                }
                            });

//            if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
//                childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
//            }

            ChannelFuture sync = serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            int port = addr.getPort();
            System.out.println("port=" + port);







        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final TlsMode tlsMode;

        private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

        HandshakeHandler(TlsMode tlsMode) {
            this.tlsMode = tlsMode;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {

            // mark the current position so that we can peek the first byte to determine if the content is starting with
            // TLS handshake
            msg.markReaderIndex();

            byte b = msg.getByte(0);

            if (b == HANDSHAKE_MAGIC_CODE) {
                switch (tlsMode) {
                    case DISABLED:
                        ctx.close();
                        System.out.println("Clients intend to establish a SSL connection while this server is running in SSL disabled mode");
                        break;
                    case PERMISSIVE:
                    case ENFORCING:
//                        if (null != sslContext) {
//                            ctx.pipeline()
//                                    .addAfter(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, TLS_HANDLER_NAME, sslContext.newHandler(ctx.channel().alloc()))
//                                    .addAfter(defaultEventExecutorGroup, TLS_HANDLER_NAME, FILE_REGION_ENCODER_NAME, new FileRegionEncoder());
//                            System.out.println("Handlers prepended to channel pipeline to establish SSL connection");
//                        } else {
//                            ctx.close();
//                            System.out.println("Trying to establish a SSL connection but sslContext is null");
//                        }
                        System.out.println("ENFORCING");
                        break;

                    default:
                        System.out.println("Unknown TLS mode");
                        break;
                }
            } else if (tlsMode == TlsMode.ENFORCING) {
                ctx.close();
                System.out.println("Clients intend to establish an insecure connection while this server is running in SSL enforcing mode");
            }

            // reset the reader index so that handshake negotiation may proceed as normal.
            msg.resetReaderIndex();

            try {
                // Remove this handler
                ctx.pipeline().remove(this);
            } catch (NoSuchElementException e) {
                System.out.println("Error while removing HandshakeHandler" + e);
            }

            // Hand over this message to the next .
            ctx.fireChannelRead(msg.retain());
        }
    }


    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            System.out.println("NETTY SERVER PIPELINE: channelRegistered {}" + remoteAddress);
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            System.out.println("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]" + remoteAddress);
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            System.out.println("NETTY SERVER PIPELINE: channelActive, the channel[{}]" + remoteAddress);
            super.channelActive(ctx);

//            if (NettyRemotingServer.this.channelEventListener != null) {
//                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));
//            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            System.out.println("NETTY SERVER PIPELINE: channelInactive, the channel[{}]" + remoteAddress);
            super.channelInactive(ctx);

//            if (NettyRemotingServer.this.channelEventListener != null) {
//                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
//            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    System.out.println("NETTY SERVER PIPELINE: IDLE exception [{}]" + remoteAddress);
                    RemotingUtil.closeChannel(ctx.channel());
//                    if (NettyRemotingServer.this.channelEventListener != null) {
//                        NettyRemotingServer.this
//                                .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
//                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            System.out.println("NETTY SERVER PIPELINE: exceptionCaught {}" + remoteAddress);
            System.out.println("NETTY SERVER PIPELINE: exceptionCaught exception." + cause);

//            if (NettyRemotingServer.this.channelEventListener != null) {
//                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
//            }

            RemotingUtil.closeChannel(ctx.channel());
        }
    }



    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }

        public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            final RemotingCommand cmd = msg;
            if (cmd != null) {
                switch (cmd.getType()) {
                    case REQUEST_COMMAND:
                        processRequestCommand(ctx, cmd);
                        break;
                    case RESPONSE_COMMAND:
                        processResponseCommand(ctx, cmd);
                        break;
                    default:
                        break;
                }
            }
        }


        /**
         * Process incoming request command issued by remote peer.
         *
         * @param ctx channel handler context.
         * @param cmd request command.
         */
        public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            ctx.writeAndFlush(response);
            System.out.println(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }

        /**
         * Process response from remote peer to the previous issued requests.
         *
         * @param ctx channel handler context.
         * @param cmd response command instance.
         */
        public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
            final int opaque = cmd.getOpaque();

            System.out.println("processResponseCommand--->");
            System.out.println(cmd);

//            final ResponseFuture responseFuture = responseTable.get(opaque);
//            if (responseFuture != null) {
//                responseFuture.setResponseCommand(cmd);
//
//                responseTable.remove(opaque);
//
//                if (responseFuture.getInvokeCallback() != null) {
//                    executeInvokeCallback(responseFuture);
//                } else {
//                    responseFuture.putResponse(cmd);
//                    responseFuture.release();
//                }
//            } else {
//                System.out.println("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
//                System.out.println(cmd.toString());
//            }
        }

        /**
         * Execute callback in callback executor. If callback executor is null, run directly in current thread
         */
        private void executeInvokeCallback(final ResponseFuture responseFuture) {
            boolean runInThisThread = false;

            ExecutorService executor = Executors.newFixedThreadPool(4, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
                }
            });

            if (executor != null) {
                try {
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                responseFuture.executeInvokeCallback();
                            } catch (Throwable e) {
                                System.out.println("execute callback in executor exception, and callback throw" + e);
                            } finally {
                                responseFuture.release();
                            }
                        }
                    });
                } catch (Exception e) {
                    runInThisThread = true;
                    System.out.println("execute callback in executor exception, maybe executor busy" + e);
                }
            } else {
                runInThisThread = true;
            }

            if (runInThisThread) {
                try {
                    responseFuture.executeInvokeCallback();
                } catch (Throwable e) {
                    System.out.println("executeInvokeCallback Exception" + e);
                } finally {
                    responseFuture.release();
                }
            }
        }




    }





}



