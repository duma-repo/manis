package com.cnblogs.duma.protocolPB;

import com.cnblogs.duma.protocol.ClientProtocol;

public class ClientManisDbProtocolTranslatorPB implements
        ClientProtocol {
    private ClientManisDbProtocolPB rpcProxy;

    public ClientManisDbProtocolTranslatorPB(ClientManisDbProtocolPB proxy) {
        rpcProxy = proxy;
    }

    @Override
    public int getTableCount(String dbName, String tbName) {
        return 0;
    }
}
