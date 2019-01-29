package org.apache.rocketmq.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.GetProducerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.*;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: codefans
 * @date: 2018-11-20 17:37
 * 参考：
 *      NettyRemotingClient
 */
public class RocketMQNettyClient {

    //线程组
    private static final EventLoopGroup group = new NioEventLoopGroup();

    //启动类
    private static final Bootstrap bootstrap = new Bootstrap();

    private static final int PORT = 3456;

    private static final String HOST = "localhost";

    private final String producerGroup = "";

    public static void main(String[] args) {
        RocketMQNettyClient rocketMQNettyClient = new RocketMQNettyClient();
        rocketMQNettyClient.startup();
        rocketMQNettyClient.sendMsg();
    }

    public void startup() {

        try {

            int connectTimeoutMillis = 3000;
            int socketSndbufSize = 65535;
            int socketRcvbufSize = 65535;
            final int clientChannelMaxIdleTimeSeconds = 120;

            int clientWorkerThreads = Runtime.getRuntime().availableProcessors();

            final DefaultEventExecutorGroup defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                    clientWorkerThreads,
                    new ThreadFactory() {
                        private AtomicInteger threadIndex = new AtomicInteger(0);
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                        }
                    });

            EventLoopGroup eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
                }
            });

            Bootstrap handler = bootstrap.group(eventLoopGroupWorker).channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
                    .option(ChannelOption.SO_SNDBUF, socketSndbufSize)
                    .option(ChannelOption.SO_RCVBUF, socketRcvbufSize)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(
                                    defaultEventExecutorGroup,
                                    new NettyEncoder(),
                                    new NettyDecoder(),
                                    new IdleStateHandler(0, 0, clientChannelMaxIdleTimeSeconds),
                                    new NettyConnectManageHandler(),
                                    new NettyClientHandler());
                        }
                    });

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
    }

    public void sendMsg() {

        ChannelFuture channelFuture = bootstrap.connect(new InetSocketAddress(HOST, PORT));
//            ChannelFuture channelFuture = bootstrap.connect().sync();
//            channel.closeFuture().sync();

        Channel channel = channelFuture.channel();
        GetProducerConnectionListRequestHeader requestHeader = new GetProducerConnectionListRequestHeader();
        requestHeader.setProducerGroup(producerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, requestHeader);

        /**
         * 创建一个ByteBuf
         */
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(0x16);
        channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                if (f.isSuccess()) {
                    System.out.println("success");
                    return;
                } else {
                    System.out.println("send request ok=false");
                }
            }
        });

        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                System.out.println("future.close():" + future.isSuccess());
            }
        });

    }


}

class NettyConnectManageHandler extends ChannelDuplexHandler {
    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
                        ChannelPromise promise) throws Exception {
        final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
        final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
        System.out.println("NETTY CLIENT PIPELINE: CONNECT  {} => {}" + local + remote);

        super.connect(ctx, remoteAddress, localAddress, promise);

//        if (NettyRemotingClient.this.channelEventListener != null) {
//            NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
//        }

    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
        System.out.println("NETTY CLIENT PIPELINE: DISCONNECT {}" + remoteAddress);
        closeChannel(ctx.channel());
        super.disconnect(ctx, promise);

//        if (NettyRemotingClient.this.channelEventListener != null) {
//            NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
//        }

    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
        System.out.println("NETTY CLIENT PIPELINE: CLOSE {}" + remoteAddress);
        closeChannel(ctx.channel());
        super.close(ctx, promise);
//        NettyRemotingClient.this.failFast(ctx.channel());
//        if (NettyRemotingClient.this.channelEventListener != null) {
//            NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
//        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                System.out.println("NETTY CLIENT PIPELINE: IDLE exception [{}]" + remoteAddress);
                closeChannel(ctx.channel());
//                if (NettyRemotingClient.this.channelEventListener != null) {
//                    NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
//                }
            }
        }

        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
        System.out.println("NETTY CLIENT PIPELINE: exceptionCaught {}" + remoteAddress);
        System.out.println("NETTY CLIENT PIPELINE: exceptionCaught exception." + cause);
        closeChannel(ctx.channel());
//        if (NettyRemotingClient.this.channelEventListener != null) {
//            NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
//        }
    }

    public static void closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                System.out.println("closeChannel: close the connection to remote address[{}] result: {}" + addrRemote + future.isSuccess());
            }
        });
    }

}

class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

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


