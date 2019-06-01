package com.cnblogs.duma;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.ipc.RPC;
import com.cnblogs.duma.protocol.ClientManisDbProtocolSerializable;
import com.cnblogs.duma.protocol.ClientProtocol;
import com.cnblogs.duma.server.ManisDb;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;


/**
 * @author duma
 */
public class ManisDbProxies {

    public static class ProxyInfo<PROXYTYPE> {
        private final PROXYTYPE proxy;
        private final InetSocketAddress address;

        public ProxyInfo(PROXYTYPE proxy, InetSocketAddress address) {
            this.proxy = proxy;
            this.address = address;
        }

        public PROXYTYPE getProxy() {
            return proxy;
        }

        public InetSocketAddress getAddress() {
            return address;
        }
    }


    private static ClientProtocol createManisDbProxyWithClientProtocol(Configuration conf,
        InetSocketAddress address)
        throws IOException {
        //todo 1. 获取配置 2. 根据配置获取代理 RPC.getProxy(...)  默认先用Serializable处理

        final long version = RPC.getProtocolVersion(ClientManisDbProtocolSerializable.class);
        int rpcTimeOut = 6000;
        ClientManisDbProtocolSerializable proxy =
                RPC.getProtocolProxy(ClientManisDbProtocolSerializable.class,
                        version, address, conf,
                        SocketFactory.getDefault(), rpcTimeOut);

        //todo serializable 使用ClientProtocol代理， protobuf 需要用装饰类
        return proxy;
    }

    @SuppressWarnings("unchecked")
    public static <T> ProxyInfo<T> createProxy(Configuration conf,
        URI uri, Class<T> xface)
        throws IOException {
        InetSocketAddress manisDbAddr = ManisDb.getAddress(uri);

        T proxy;
        if (xface == ClientProtocol.class) {
            proxy =  (T) createManisDbProxyWithClientProtocol(conf, manisDbAddr);
        } else {
            String message = "Unsupported protocol found when creating the proxy " +
                    "connection to ManisDb: " +
                    ((xface != null) ? xface.getName() : "null");
            throw new IllegalStateException(message);
        }

        return new ProxyInfo<T>(proxy, manisDbAddr);
    }
}
