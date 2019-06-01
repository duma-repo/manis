package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 实现 RPC 的接口
 * @author duma
 */
public interface RpcEngine {

    /**
     * 构造客户端的代理对象
     * @param protocol
     * @param clientVersion
     * @param address
     * @param conf
     * @param factory
     * @param rpcTimeOut
     * @param <T>
     * @return
     * @throws IOException
     */
    <T> T getProxy(Class<T> protocol,
                   long clientVersion,
                   InetSocketAddress address,
                   Configuration conf,
                   SocketFactory factory,
                   int rpcTimeOut) throws IOException;
}
