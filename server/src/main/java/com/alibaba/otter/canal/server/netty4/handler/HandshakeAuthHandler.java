package com.alibaba.otter.canal.server.netty4.handler;

import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitors;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.TimeUnit;

import static com.alibaba.otter.canal.server.netty.NettyUtils.VERSION;

/**
 * @author kerryzhang on 2022/8/12
 */


public class HandshakeAuthHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(HandshakeAuthHandler.class);
    private final int SUPPORTED_VERSION = 3;

    private final int defaultSubscriptorDisconnectIdleTimeout = 60 * 60 * 1000;

    // support to maintain socket channel.
    private final ChannelGroup childGroups;
    private byte[] seed;
    private CanalServerWithEmbedded embeddedServer;

    public HandshakeAuthHandler(ChannelGroup childGroups) {
        this.childGroups = childGroups;
    }

    public HandshakeAuthHandler(ChannelGroup childGroups, CanalServerWithEmbedded embeddedServer) {
        this.childGroups = childGroups;
        this.embeddedServer = embeddedServer;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // add new socket channel in channel container, used to manage sockets.
        if (childGroups != null) {
            childGroups.add(ctx.channel());
        }
        seed = org.apache.commons.lang3.RandomUtils.nextBytes(8);
        byte[] body = CanalPacket.Packet.newBuilder()
                .setType(CanalPacket.PacketType.HANDSHAKE)
                .setVersion(VERSION)
                .setBody(CanalPacket.Handshake.newBuilder().setSeeds(ByteString.copyFrom(seed)).build().toByteString())
                .build()
                .toByteArray();

        ctx.channel().writeAndFlush(body);

        logger.info("send handshake initialization packet to : {}", ctx.channel());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        byte[] array = new byte[buf.readableBytes()];
        buf.readBytes(array);
        CanalPacket.Packet packet = CanalPacket.Packet.parseFrom(array);
        switch (packet.getVersion()) {
            case SUPPORTED_VERSION:
            default:
                final CanalPacket.ClientAuth clientAuth = CanalPacket.ClientAuth.parseFrom(packet.getBody());
                if (seed == null) {
                    // error
                    return;
                }
                if (!embeddedServer.auth(clientAuth.getUsername(), clientAuth.getPassword().toStringUtf8(), seed)) {
                    return;
                }
                // 如果存在订阅信息
                if (StringUtils.isNotEmpty(clientAuth.getDestination()) && StringUtils.isNotEmpty(clientAuth.getClientId())) {
                    ClientIdentity clientIdentity = new ClientIdentity(clientAuth.getDestination(), Short.parseShort(clientAuth.getClientId()), clientAuth.getFilter());
                    try {
                        MDC.put("destination", clientIdentity.getDestination());
                        embeddedServer.subscribe(clientIdentity);
                        // 尝试启动，如果已经启动，忽略
                        if (!embeddedServer.isStart(clientIdentity.getDestination())) {
                            ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
                            if (!runningMonitor.isStart()) {
                                runningMonitor.start();
                            }
                        }
                    } finally {
                        MDC.remove("destination");
                    }
                }
                ack(ctx.channel(), channelFuture -> {
                    logger.info("remove unused channel handlers after authentication is done successfully.");
                    ctx.pipeline().remove(HandshakeAuthHandler.class.getName());

                    int readTimeout = defaultSubscriptorDisconnectIdleTimeout;
                    int writeTimeout = defaultSubscriptorDisconnectIdleTimeout;
                    if (clientAuth.getNetReadTimeout() > 0) {
                        readTimeout = clientAuth.getNetReadTimeout();
                    }
                    if (clientAuth.getNetWriteTimeout() > 0) {
                        writeTimeout = clientAuth.getNetWriteTimeout();
                    }
                    // fix bug: soTimeout parameter's unit from connector is millseconds.
                    IdleHandler idleStateHandler = new IdleHandler(readTimeout, writeTimeout, 0, TimeUnit.MILLISECONDS);
                    ctx.pipeline().addBefore(SessionHandler4.class.getName(), IdleHandler.class.getName(), idleStateHandler);
                });
                break;
        }
    }

    public static void ack(Channel channel, ChannelFutureListener channelFutureListner) {
        byte[] ack = CanalPacket.Packet.newBuilder()
                .setType(CanalPacket.PacketType.ACK)
                .setVersion(VERSION)
                .setBody(CanalPacket.Ack.newBuilder().build().toByteString())
                .build()
                .toByteArray();
        channel.writeAndFlush(ack).addListener(channelFutureListner);
    }

    public void setEmbeddedServer(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }
}
