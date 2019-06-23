package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.Writable;

import javax.net.SocketFactory;
import java.io.IOException;

public class Client {
    private Class<? extends Writable> valueClass;
    final private Configuration conf;
    private SocketFactory factory;

    public Client(Class<? extends Writable> valueClass, Configuration conf,
                  SocketFactory factory) {
        this.valueClass = valueClass;
        this.conf = conf;
        this.factory = factory;
    }

    public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest, ConnectionId remoteId)
        throws IOException {
        return null;
    }


    public static class ConnectionId {

    }
}
