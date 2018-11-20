package org.apache.rocketmq.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: ShengzhiCai
 * @date: 2018-11-20 17:37
 * 参考：
 *      NettyRemotingClient
 */
public class RocketMQNettyClient {

    //线程组
    private static final EventLoopGroup group = new NioEventLoopGroup();

    //启动类
    private static final Bootstrap bootstrap = new Bootstrap();

    private static final int PORT = 6789;

    private static final String HOST = "localhost";

    public static void main(String[] args) {
        RocketMQNettyClient rocketMQNettyClient = new RocketMQNettyClient();
        rocketMQNettyClient.startup();
    }

    public void startup() {

        try {

            int connectTimeoutMillis = 3000;
            int socketSndbufSize = 65535;
            int socketRcvbufSize = 65535;

            EventLoopGroup group = new NioEventLoopGroup(1, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
                }
            });

            bootstrap.group(group).channel(NioSocketChannel.class)
//                    .remoteAddress(new InetSocketAddress(HOST, PORT))
                    .option(ChannelOption.TCP_NODELAY, true)
                    //长连接
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
                    .option(ChannelOption.SO_SNDBUF, socketSndbufSize)
                    .option(ChannelOption.SO_RCVBUF, socketRcvbufSize)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception {

                            ChannelPipeline pipeline = channel.pipeline();

                            //包含编码器和解码器
                            pipeline.addLast(new HttpClientCodec());

                            //聚合
                            pipeline.addLast(new HttpObjectAggregator(1024 * 10 * 1024));

                            //解压
                            pipeline.addLast(new HttpContentDecompressor());

                            pipeline.addLast(new ClientHandler());
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
        channel.writeAndFlush("1234567890123456789").addListener(new ChannelFutureListener() {
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
                System.out.println("future.isSuccess():" + future.isSuccess());
            }
        });

    }


}





/**
 * 客户端处理器
 */
class ClientHandler extends ChannelInboundHandlerAdapter {


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        FullHttpResponse response = (FullHttpResponse) msg;

        ByteBuf content = response.content();
        HttpHeaders headers = response.headers();

        System.out.println("content:" + System.getProperty("line.separator") + content.toString(CharsetUtil.UTF_8));
        System.out.println("headers:" + System.getProperty("line.separator") + headers.toString());
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        URI url = new URI("/test");
        String meg = "hello";

        //配置HttpRequest的请求数据和一些配置信息
        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_0, HttpMethod.GET, url.toASCIIString(), Unpooled.wrappedBuffer(meg.getBytes("UTF-8")));

        request.headers()
                .set(HttpHeaders.Names.CONTENT_TYPE, "text/plain;charset=UTF-8")
                //开启长连接
                .set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
                //设置传递请求内容的长度
                .set(HttpHeaders.Names.CONTENT_LENGTH, request.content().readableBytes());

        //发送数据
        ctx.writeAndFlush(request);
    }
}
