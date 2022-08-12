package com.alibaba.otter.canal.server.netty4;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.server.CanalServer;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty4.handler.HandshakeAuthHandler;
import com.alibaba.otter.canal.server.netty4.handler.SessionHandler4;
import com.alibaba.otter.canal.server.netty4.handler.WrapPacketLenOutBoundHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.lang.StringUtils;

import java.net.InetSocketAddress;


/**
 * @author kerryzhang on 2022/8/12
 */


public class CanalServerWithNetty4 extends AbstractCanalLifeCycle implements CanalServer {
    private CanalServerWithEmbedded embeddedServer;
    private String ip;
    private int port;
    private Channel serverChannel = null;
    private ChannelGroup childGroups = null;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workGroup;

    private static class SingletonHolder {
        private static final CanalServerWithNetty4 CANAL_SERVER_WITH_NETTY = new CanalServerWithNetty4();
    }

    public static CanalServerWithNetty4 instance() {
        return CanalServerWithNetty4.SingletonHolder.CANAL_SERVER_WITH_NETTY;
    }

    private CanalServerWithNetty4() {
        this.embeddedServer = CanalServerWithEmbedded.instance();
        this.childGroups = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    }


    @Override
    public void start() {
        super.start();
        if (!embeddedServer.isStart()) {
            embeddedServer.start();
        }

        bossGroup = new NioEventLoopGroup(1);
        workGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap().group(bossGroup, workGroup);

        /*
         * enable keep-alive mechanism, handle abnormal network connection
         * scenarios on OS level. the threshold parameters are depended on OS.
         * e.g. On Linux: net.ipv4.tcp_keepalive_time = 300
         * net.ipv4.tcp_keepalive_probes = 2 net.ipv4.tcp_keepalive_intvl = 30
         */
        // bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        /*
         * optional parameter.
         */
        // bootstrap.option(ChannelOption.TCP_NODELAY, true);
        // 构造对应的pipeline

        bootstrap.channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();

                        // pipeline.addLast(new LengthFieldBasedFrameDecoder(10240, 0, 4, 0, 4));
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(10240, 0, 4, 0, 0));
                        pipeline.addLast(new LoggingHandler(LogLevel.DEBUG));

                        pipeline.addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                ByteBuf buf = (ByteBuf) msg;
                                buf.readInt();
                                super.channelRead(ctx, buf);
                            }
                        });

                        pipeline.addLast(HandshakeAuthHandler.class.getName(), new HandshakeAuthHandler(childGroups, embeddedServer));

                        pipeline.addLast(SessionHandler4.class.getName(), new SessionHandler4(embeddedServer));

                        pipeline.addLast(new WrapPacketLenOutBoundHandler());
                    }
                });
        try {
            if (StringUtils.isNotEmpty(ip)) {
                serverChannel = bootstrap.bind(new InetSocketAddress(ip, port)).sync().channel();
            } else {
                serverChannel = bootstrap.bind(new InetSocketAddress(port)).sync().channel();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public void stop() {
        super.stop();

        if (this.serverChannel != null) {
            this.serverChannel.close().awaitUninterruptibly(1000L);
        }
        if (this.childGroups != null) {
            this.childGroups.close().awaitUninterruptibly(1000L);
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workGroup != null) {
            workGroup.shutdownGracefully();
        }
        if (embeddedServer.isStart()) {
            embeddedServer.stop();
        }
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setEmbeddedServer(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }
}
