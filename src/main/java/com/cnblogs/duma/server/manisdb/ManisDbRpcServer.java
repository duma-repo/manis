package com.cnblogs.duma.server.manisdb;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.ipc.ProtobufRpcEngine;
import com.cnblogs.duma.ipc.RPC;
import com.cnblogs.duma.protocolPB.ClientManisDbProtocolPB;
import com.cnblogs.duma.protocolPB.ClientManisdbProtocolServerSideTranslatorPB;
import com.cnblogs.duma.server.protocol.ManisDbProtocols;

import java.io.IOException;

/**
 * 该类处理所有 ManisDb 的 rpc 调用
 * 它由 {@link ManisDb} 创建、启动和停止
 * @author duma
 */
public class ManisDbRpcServer implements ManisDbProtocols {
    public ManisDbRpcServer(Configuration conf) {

        RPC.setProtocolEngine(conf, ClientManisDbProtocolPB.class,
                ProtobufRpcEngine.class);

        ClientManisdbProtocolServerSideTranslatorPB
                clientProtocolServerTranslator = new ClientManisdbProtocolServerSideTranslatorPB(this);

    }

    @Override
    public int getTableCount(String dbName, String tbName) throws IOException {
        return 0;
    }

    @Override
    public boolean setMaxTable(int tableNum) {
        return false;
    }
}
