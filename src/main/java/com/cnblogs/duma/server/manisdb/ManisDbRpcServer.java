package com.cnblogs.duma.server.manisdb;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.ipc.ProtobufRpcEngine;
import com.cnblogs.duma.ipc.RPC;
import com.cnblogs.duma.protocol.proto.ClientManisDbProtocolProtos.ClientManisDbProtocol;
import com.cnblogs.duma.protocolPB.ClientManisDbProtocolPB;
import com.cnblogs.duma.protocolPB.ClientManisdbProtocolServerSideTranslatorPB;
import com.cnblogs.duma.server.protocol.ManisDbProtocols;
import com.google.protobuf.BlockingService;
import org.apache.commons.logging.Log;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 该类处理所有 ManisDb 的 rpc 调用
 * 它由 {@link ManisDb} 创建、启动和停止
 * @author duma
 */
public class ManisDbRpcServer implements ManisDbProtocols {
    private static final Log LOG = ManisDb.LOG;

    /** 处理客户端 protobuf rpc 调用的 Server */
    protected final RPC.Server protoBufRpcServer;

    public ManisDbRpcServer(Configuration conf) throws IOException {
        int handlerCount =
                conf.getInt(CommonConfigurationKeysPublic.MANIS_HANDLER_COUNT_KEY,
                        CommonConfigurationKeysPublic.MANIS_HANDLER_COUNT_DEFAULT);

        RPC.setProtocolEngine(conf, ClientManisDbProtocolPB.class,
                ProtobufRpcEngine.class);

        ClientManisdbProtocolServerSideTranslatorPB
                clientProtocolServerTranslator = new ClientManisdbProtocolServerSideTranslatorPB(this);
        BlockingService clientMdPbService =
                ClientManisDbProtocol.newReflectiveBlockingService(clientProtocolServerTranslator);

        InetSocketAddress protoBufRpcServerAddr = ManisDb.getProtoBufRpcServerAddress(conf);
        String bindHost = protoBufRpcServerAddr.getHostName();
        int bindPort = protoBufRpcServerAddr.getPort();
        LOG.info("RPC server is binding to " + bindHost + ":" + bindPort);
        this.protoBufRpcServer = new RPC.Builder(conf)
                .setProtocol(ClientManisDbProtocolPB.class)
                .setInstance(clientMdPbService)
                .setBindAdress(bindHost)
                .setBindPort(bindPort)
                .setNumHandlers(handlerCount)
                .setVerbose(true)
                .build();
    }

    /**
     * 启动 RPC 服务端
     */
    void start() {
        protoBufRpcServer.start();
    }

    /**
     * 等待 RPC 服务停止
     */
    void join() throws InterruptedException {
        protoBufRpcServer.join();
    }

    @Override
    public int getTableCount(String dbName, String tbName)
            throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("GET table count for db: " + dbName + " tb: " + tbName);
        }
        return 10;
    }

    @Override
    public boolean setMaxTable(int tableNum) {
        return false;
    }
}
