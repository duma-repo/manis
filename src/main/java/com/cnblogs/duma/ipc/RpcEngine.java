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

    /**
     * 构造一个 Server 实例
     * @param protocol 协议/接口
     * @param instance 实现 protocol 的实例
     * @param bindAddress 服务端地址
     * @param port 服务端端口
     * @param numHandlers handler 线程个数
     * @param numReaders reader 线程个数
     * @param queueSizePerHandler 每一个 handler 线程队列大小
     * @param verbose 是否对调用信息打log
     * @param conf configuration
     * @return RPC.Server 实例
     * @throws IOException
     */
    RPC.Server getServer(Class<?> protocol, Object instance, String bindAddress,
                         int port, int numHandlers, int numReaders,
                         int queueSizePerHandler, boolean verbose,
                         Configuration conf
                        ) throws IOException;
}
