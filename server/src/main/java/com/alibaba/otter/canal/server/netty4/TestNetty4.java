package com.alibaba.otter.canal.server.netty4;

import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;

/**
 * @author kerryzhang on 2022/8/12
 */


public class TestNetty4 {
    public static void main(String[] args) {
        CanalServerWithNetty4 instance = CanalServerWithNetty4.instance();
        instance.setPort(12345);
        CanalServerWithEmbedded serverWithEmbedded = CanalServerWithEmbedded.instance();
        serverWithEmbedded.start();
        instance.setEmbeddedServer(serverWithEmbedded);
        instance.start();
    }
}
