package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;

public class ProtobufRpcEngine implements RpcEngine {
    @Override
    public <T> T getProxy(Class<T> protocol, long clientVersion, InetSocketAddress address, Configuration conf, SocketFactory factory, int rpcTimeOut) throws IOException {
        return null;
    }
}
