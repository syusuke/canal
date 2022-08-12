package com.alibaba.otter.canal.server.netty4.handler;

import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.NettyUtils;
import com.alibaba.otter.canal.server.netty.listener.ChannelFutureAggregator;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.WireFormat;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author kerryzhang on 2022/8/12
 */


public class SessionHandler4 extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(SessionHandler4.class);
    private CanalServerWithEmbedded embeddedServer;

    public SessionHandler4() {

    }

    public SessionHandler4(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logger.info("message receives in session handler...");
        long start = System.nanoTime();
        ByteBuf buf = (ByteBuf) msg;
        byte[] array = new byte[buf.readableBytes()];
        buf.readBytes(array);
        CanalPacket.Packet packet = CanalPacket.Packet.parseFrom(array);
        ClientIdentity clientIdentity = null;
        logger.info("packet: {}", packet);
        try {
            switch (packet.getType()) {
                case SUBSCRIPTION:
                    subscription(ctx, packet, start);
                    break;
                case UNSUBSCRIPTION:
                    unsubscription(ctx, packet, start);
                    break;
                case GET:
                    get(ctx, packet, start);
                    break;
                case CLIENTACK:
                    clientAck(ctx, packet, start);
                    break;
                case CLIENTROLLBACK:
                    clientRollback(ctx, packet, start);
                default:
                    byte[] errorBytes = NettyUtils.errorPacket(400,
                            MessageFormatter.format("packet type={} is NOT supported!", packet.getType()).getMessage());
                    ctx.channel().writeAndFlush(errorBytes);
                    break;
            }
        } catch (Throwable exception) {
            byte[] errorBytes = NettyUtils.errorPacket(400,
                    MessageFormatter.format("something goes wrong with channel:{}, exception={}",
                            ctx.channel(),
                            ExceptionUtils.getStackTrace(exception)).getMessage());
            ctx.channel().writeAndFlush(errorBytes);
        } finally {
            MDC.remove("destination");
        }
    }

    private void clientRollback(ChannelHandlerContext ctx, CanalPacket.Packet packet, long start) throws IOException {
        CanalPacket.ClientRollback rollback = CanalPacket.ClientRollback.parseFrom(packet.getBody());
        MDC.put("destination", rollback.getDestination());
        if (StringUtils.isNotEmpty(rollback.getDestination())
                && StringUtils.isNotEmpty(rollback.getClientId())) {
            ClientIdentity clientIdentity = new ClientIdentity(rollback.getDestination(), Short.parseShort(rollback.getClientId()));
            if (rollback.getBatchId() == 0L) {
                embeddedServer.rollback(clientIdentity);// 回滚所有批次
            } else {
                embeddedServer.rollback(clientIdentity, rollback.getBatchId()); // 只回滚单个批次
            }
            new ChannelFutureAggregator(rollback.getDestination(),
                    rollback,
                    packet.getType(),
                    0,
                    System.nanoTime() - start).operationComplete(null);
        } else {
            byte[] errorBytes = NettyUtils.errorPacket(401,
                    MessageFormatter.format("destination or clientId is null", rollback.toString())
                            .getMessage());
            ctx.channel().writeAndFlush(errorBytes);
        }
    }

    private void clientAck(ChannelHandlerContext ctx, CanalPacket.Packet packet, long start) throws IOException {
        CanalPacket.ClientAck ack = CanalPacket.ClientAck.parseFrom(packet.getBody());
        MDC.put("destination", ack.getDestination());
        if (StringUtils.isNotEmpty(ack.getDestination()) && StringUtils.isNotEmpty(ack.getClientId())) {
            if (ack.getBatchId() == 0L) {
                byte[] errorBytes = NettyUtils.errorPacket(402,
                        MessageFormatter.format("batchId should assign value", ack.toString()).getMessage());
                ctx.channel().writeAndFlush(errorBytes);
            } else if (ack.getBatchId() == -1L) { // -1代表上一次get没有数据，直接忽略之
                // donothing
            } else {
                ClientIdentity clientIdentity = new ClientIdentity(ack.getDestination(), Short.valueOf(ack.getClientId()));
                embeddedServer.ack(clientIdentity, ack.getBatchId());
                new ChannelFutureAggregator(ack.getDestination(),
                        ack,
                        packet.getType(),
                        0,
                        System.nanoTime() - start).operationComplete(null);
            }
        } else {
            byte[] errorBytes = NettyUtils.errorPacket(401,
                    MessageFormatter.format("destination or clientId is null", ack.toString()).getMessage());

            ctx.channel().writeAndFlush(errorBytes);
        }
    }

    private void get(ChannelHandlerContext ctx, CanalPacket.Packet packet, long start) throws IOException {
        CanalPacket.Get get = CanalPacket.Get.parseFrom(packet.getBody());
        if (StringUtils.isNotEmpty(get.getDestination()) && StringUtils.isNotEmpty(get.getClientId())) {
            ClientIdentity clientIdentity = new ClientIdentity(get.getDestination(), Short.parseShort(get.getClientId()));
            MDC.put("destination", clientIdentity.getDestination());
            Message message = null;

            // if (get.getAutoAck()) {
            // if (get.getTimeout() == -1) {//是否是初始值
            // message = embeddedServer.get(clientIdentity,
            // get.getFetchSize());
            // } else {
            // TimeUnit unit = convertTimeUnit(get.getUnit());
            // message = embeddedServer.get(clientIdentity,
            // get.getFetchSize(), get.getTimeout(), unit);
            // }
            // } else {
            if (get.getTimeout() == -1) {// 是否是初始值
                message = embeddedServer.getWithoutAck(clientIdentity, get.getFetchSize());
            } else {
                TimeUnit unit = convertTimeUnit(get.getUnit());
                message = embeddedServer.getWithoutAck(clientIdentity,
                        get.getFetchSize(),
                        get.getTimeout(),
                        unit);
            }
            // }

            if (message.getId() != -1 && message.isRaw()) {
                List<ByteString> rowEntries = message.getRawEntries();
                // message size
                int messageSize = 0;
                messageSize += com.google.protobuf.CodedOutputStream.computeInt64Size(1, message.getId());

                int dataSize = 0;
                for (ByteString rowEntry : rowEntries) {
                    dataSize += CodedOutputStream.computeBytesSizeNoTag(rowEntry);
                }
                messageSize += dataSize;
                messageSize += 1 * rowEntries.size();
                // packet size
                int size = 0;
                size += com.google.protobuf.CodedOutputStream.computeEnumSize(3,
                        CanalPacket.PacketType.MESSAGES.getNumber());
                size += com.google.protobuf.CodedOutputStream.computeTagSize(5)
                        + com.google.protobuf.CodedOutputStream.computeRawVarint32Size(messageSize)
                        + messageSize;
                // recyle bytes
                // ByteBuffer byteBuffer = (ByteBuffer)
                // ctx.getAttachment();
                // if (byteBuffer != null && size <=
                // byteBuffer.capacity()) {
                // byteBuffer.clear();
                // } else {
                // byteBuffer =
                // ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
                // ctx.setAttachment(byteBuffer);
                // }
                // CodedOutputStream output =
                // CodedOutputStream.newInstance(byteBuffer);
                byte[] body = new byte[size];
                CodedOutputStream output = CodedOutputStream.newInstance(body);
                output.writeEnum(3, CanalPacket.PacketType.MESSAGES.getNumber());

                output.writeTag(5, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                output.writeUInt32NoTag(messageSize);
                // message
                output.writeInt64(1, message.getId());
                for (ByteString rowEntry : rowEntries) {
                    output.writeBytes(2, rowEntry);
                }
                output.checkNoSpaceLeft();

                ctx.channel().writeAndFlush(body);

                // output.flush();
                // byteBuffer.flip();
                // NettyUtils.write(ctx.getChannel(), byteBuffer,
                // null);
            } else {
                CanalPacket.Packet.Builder packetBuilder = CanalPacket.Packet.newBuilder();
                packetBuilder.setType(CanalPacket.PacketType.MESSAGES).setVersion(NettyUtils.VERSION);

                CanalPacket.Messages.Builder messageBuilder = CanalPacket.Messages.newBuilder();
                messageBuilder.setBatchId(message.getId());
                if (message.getId() != -1) {
                    if (message.isRaw() && !CollectionUtils.isEmpty(message.getRawEntries())) {
                        messageBuilder.addAllMessages(message.getRawEntries());
                    } else if (!CollectionUtils.isEmpty(message.getEntries())) {
                        for (CanalEntry.Entry entry : message.getEntries()) {
                            messageBuilder.addMessages(entry.toByteString());
                        }
                    }
                }
                byte[] body = packetBuilder.setBody(messageBuilder.build().toByteString())
                        .build()
                        .toByteArray();
                ctx.channel().writeAndFlush(body);
            }
        } else {
            byte[] errorBytes = NettyUtils.errorPacket(401,
                    MessageFormatter.format("destination or clientId is null", get.toString()).getMessage());
            ctx.channel().writeAndFlush(errorBytes);
        }
    }


    private void subscription(ChannelHandlerContext ctx, CanalPacket.Packet packet, long start) throws InvalidProtocolBufferException {
        CanalPacket.Sub sub = CanalPacket.Sub.parseFrom(packet.getBody());
        logger.info("subscription: {}", sub);
        if (StringUtils.isNotEmpty(sub.getDestination()) && StringUtils.isNotEmpty(sub.getClientId())) {
            ClientIdentity clientIdentity = new ClientIdentity(sub.getDestination(), Short.parseShort(sub.getClientId()), sub.getFilter());
            MDC.put("destination", clientIdentity.getDestination());


            embeddedServer.subscribe(clientIdentity);
            byte[] ackBytes = NettyUtils.ackPacket();

            ctx.channel().writeAndFlush(ackBytes);
        } else {
            byte[] errorBytes = NettyUtils.errorPacket(401, MessageFormatter.format("destination or clientId is null", sub.toString()).getMessage());
            ctx.channel().writeAndFlush(errorBytes);
        }
    }

    private void unsubscription(ChannelHandlerContext ctx, CanalPacket.Packet packet, long start) throws InvalidProtocolBufferException {

        CanalPacket.Unsub unsub = CanalPacket.Unsub.parseFrom(packet.getBody());
        if (StringUtils.isNotEmpty(unsub.getDestination()) && StringUtils.isNotEmpty(unsub.getClientId())) {
            ClientIdentity clientIdentity = new ClientIdentity(unsub.getDestination(),
                    Short.parseShort(unsub.getClientId()),
                    unsub.getFilter());
            MDC.put("destination", clientIdentity.getDestination());
            embeddedServer.unsubscribe(clientIdentity);
            // 尝试关闭
            stopCanalInstanceIfNecessary(clientIdentity);
            byte[] ackBytes = NettyUtils.ackPacket();
            ctx.channel().writeAndFlush(ackBytes);
        } else {
            byte[] errorBytes = NettyUtils.errorPacket(401,
                    MessageFormatter.format("destination or clientId is null", unsub.toString()).getMessage());
            ctx.channel().writeAndFlush(errorBytes);
        }
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("something goes wrong with channel:{}, exception={}",
                ctx.channel(),
                ExceptionUtils.getStackTrace(cause.getCause()));
        ctx.channel().close();
    }


    private void stopCanalInstanceIfNecessary(ClientIdentity clientIdentity) {
        List<ClientIdentity> clientIdentitys = embeddedServer.listAllSubscribe(clientIdentity.getDestination());
        if (clientIdentitys != null && clientIdentitys.size() == 1 && clientIdentitys.contains(clientIdentity)) {
            ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
            if (runningMonitor.isStart()) {
                runningMonitor.release();
            }
        }
    }

    private TimeUnit convertTimeUnit(int unit) {
        switch (unit) {
            case 0:
                return TimeUnit.NANOSECONDS;
            case 1:
                return TimeUnit.MICROSECONDS;
            case 2:
                return TimeUnit.MILLISECONDS;
            case 3:
                return TimeUnit.SECONDS;
            case 4:
                return TimeUnit.MINUTES;
            case 5:
                return TimeUnit.HOURS;
            case 6:
                return TimeUnit.DAYS;
            default:
                return TimeUnit.MILLISECONDS;
        }
    }

    public void setEmbeddedServer(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }
}
