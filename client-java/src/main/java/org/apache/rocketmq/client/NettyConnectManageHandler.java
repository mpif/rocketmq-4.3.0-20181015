package org.apache.rocketmq.client;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.rocketmq.client.util.RemotingHelper;

import java.net.SocketAddress;

import static org.apache.rocketmq.client.util.RemotingUtil.closeChannel;

/**
 * @Author: codefans
 * @Date: 2018-11-06 23:29
 */

public class NettyConnectManageHandler extends ChannelDuplexHandler {
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
//                    NettyRemotingClient.this
//                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
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
}
