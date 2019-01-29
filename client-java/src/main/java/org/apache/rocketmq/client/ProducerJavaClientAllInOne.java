package org.apache.rocketmq.client;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.apache.rocketmq.client.exception.RemotingSendRequestException;
import org.apache.rocketmq.client.util.RemotingHelper;
import org.apache.rocketmq.client.util.RemotingUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: codefans
 * @Date: 2018-11-03 18:40
 */

public class ProducerJavaClientAllInOne {

    public static void main(String[] args) {
        ProducerJavaClientAllInOne producerJavaClientAllInOne = new ProducerJavaClientAllInOne();
        producerJavaClientAllInOne.produce();
    }

    public void produce() {

        this.socketProduce();

        this.nettyProduce();


    }

    public void socketProduce() {

        try {

//            String namesrvAddr = "192.168.199.159:9876";
            String host = "192.168.199.159";
            int port = 9876;
            Socket socket = new Socket(host, port);
            socket.setKeepAlive(true);
            socket.setSoTimeout(3 * 1000);
//            SocketChannel socketChannel = socket.getChannel();

            InputStream is = socket.getInputStream();
            OutputStream os = socket.getOutputStream();



        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void nettyProduce() {

        String namesrvAddr = "192.168.199.159:9876";

        int connectTimeoutMillis = 3000;

        int clientSocketSndBufSize = 65535;
        int clientSocketRcvBufSize = 65535;
        final int clientChannelMaxIdleTimeSeconds = 120;

        EventLoopGroup eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
            }
        });

        final DefaultEventExecutorGroup defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                4,
                new ThreadFactory() {
                    private AtomicInteger threadIndex = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                    }
                });

        Bootstrap bootstrap = new Bootstrap();
        Bootstrap handler = bootstrap.group(eventLoopGroupWorker).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
                .option(ChannelOption.SO_SNDBUF, clientSocketSndBufSize)
                .option(ChannelOption.SO_RCVBUF, clientSocketRcvBufSize)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
//                        if (nettyClientConfig.isUseTLS()) {
//                            if (null != sslContext) {
//                                pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
//                                log.info("Prepend SSL handler");
//                            } else {
//                                log.warn("Connections are insecure as SSLContext is null!");
//                            }
//                        }
                        pipeline.addLast(
                                defaultEventExecutorGroup,
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, clientChannelMaxIdleTimeSeconds),
                                new NettyConnectManageHandler(),
                                new NettyClientHandler());
                    }
                });


        ChannelFuture channelFuture = bootstrap.connect(RemotingHelper.string2SocketAddress(namesrvAddr));
        Channel channel = channelFuture.channel();
        if (channel != null && channel.isActive()) {
            try {
//                if (this.rpcHook != null) {
//                    this.rpcHook.doBeforeRequest(addr, request);
//                }
//                long costTime = System.currentTimeMillis() - beginStartTime;
//                if (timeoutMillis < costTime) {
//                    throw new RemotingTooMuchRequestException("invokeAsync call timeout");
//                }
//                this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, invokeCallback);

                RemotingCommand request = new RemotingCommand();
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) {
                            System.out.println("success....");
                            return;
                        }
//                        requestFail(opaque);
//                        System.out.println("send a request command to channel <{}> failed." + RemotingHelper.parseChannelRemoteAddr(channel));
                    }
                });


            } catch (Exception e) {
                System.out.println("invokeAsync: send request exception, so close the channel[{}]" + namesrvAddr);
                RemotingUtil.closeChannel(channel);
                throw e;
            }
        }


    }


}
