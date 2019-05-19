package com.cnblogs.duma;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.protocol.ClientProtocol;
import com.cnblogs.duma.server.ManisDb;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Properties;


/**
 * @author duma
 */
public class ManisDbProxies {

    public static class ProxyInfo<PROXYTYPE> {
        private final PROXYTYPE proxy;
        private final String dtService;
        private final InetSocketAddress address;

        public ProxyInfo(PROXYTYPE proxy, String dtService
                , InetSocketAddress address) {
            this.proxy = proxy;
            this.dtService = dtService;
            this.address = address;
        }

        public PROXYTYPE getProxy() {
            return proxy;
        }

        public String getDtService() {
            return dtService;
        }

        public InetSocketAddress getAddress() {
            return address;
        }
    }

    private static <T> ProxyInfo<T> createManisDbProxyWithClientProtocol(Configuration conf,
        InetSocketAddress address) {
        return null;
    }

    public static <T> ProxyInfo<T> createProxy(Configuration conf,
        URI uri, Class<T> xface)
        throws Exception {
        InetSocketAddress manisDbAddr = ManisDb.getAddress(uri);

        T proxy;
        if (xface == ClientProtocol.class) {
            proxy =  (T) createManisDbProxyWithClientProtocol(conf, manisDbAddr);
        } else {
            // todo
            throw new Exception();
        }

        return new ProxyInfo<T>(proxy, null, manisDbAddr);
    }
}
