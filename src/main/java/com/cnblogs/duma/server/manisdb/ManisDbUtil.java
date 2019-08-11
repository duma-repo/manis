package com.cnblogs.duma.server.manisdb;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.ipc.RPC;
import com.cnblogs.duma.ipc.SerializableRpcEngine;
import com.cnblogs.duma.ipc.RPC;

public class ManisDbUtil {
    public static void addSeriablizableProtocol(Configuration conf
            , Class<?> protocol, Object service, RPC.Server server) {
        try {
            // 为了加载 SerializableRpcEngine 类
            Class.forName(SerializableRpcEngine.class.getName());
        } catch (ClassNotFoundException e) {}
        RPC.setProtocolEngine(conf, protocol, SerializableRpcEngine.class);
        server.addProtocol(RPC.RpcKind.RPC_SERIALIZABLE, protocol, service);
    }
}
