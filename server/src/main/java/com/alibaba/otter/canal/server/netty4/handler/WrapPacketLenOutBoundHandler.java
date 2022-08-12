package com.alibaba.otter.canal.server.netty4.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/**
 * @author kerryzhang on 2022/8/12
 */


public class WrapPacketLenOutBoundHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof byte[]) {
            byte[] data = (byte[]) msg;
            ByteBuf len = Unpooled.copyInt(data.length);
            len.writeBytes(data);
            super.write(ctx, len, promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }
}
