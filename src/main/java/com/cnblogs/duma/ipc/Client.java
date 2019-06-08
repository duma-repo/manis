package com.cnblogs.duma.ipc;

import com.cnblogs.duma.io.Writable;

import java.io.IOException;

public class Client {
    public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest, ConnectionId remoteId)
        throws IOException {
        return null;
    }


    public static class ConnectionId {

    }
}
