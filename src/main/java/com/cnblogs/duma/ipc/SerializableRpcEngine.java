package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

public class SerializableRpcEngine implements RpcEngine {
    public static final Log LOG = LogFactory.getLog(SerializableRpcEngine.class);

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProxy(Class<T> protocol, long clientVersion,
                          InetSocketAddress address, Configuration conf,
                          SocketFactory factory, int rpcTimeOut)
            throws IOException {

        final Invoker invoker = new Invoker(protocol, address, conf, factory, rpcTimeOut);
        return (T) Proxy.newProxyInstance(protocol.getClassLoader(), new Class[]{protocol}, invoker);
    }

    private static class Invoker implements RpcInvocationHandler {

        Invoker(Class<?> protocol, InetSocketAddress address,
                Configuration conf, SocketFactory factory,
                int rpcTimeOut)
                throws IOException {
            System.out.println("init Invoker");
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return null;
        }
    }
}
