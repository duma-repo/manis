package com.cnblogs.duma;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.protocol.ClientProtocol;
import com.cnblogs.duma.protocol.ManagerProtocol;

import java.io.IOException;
import java.net.URI;

public class Manager {
    final ManagerProtocol manisDb;

    public Manager(URI manisDbUri, Configuration conf) throws IOException {
        ManisDbProxies.ProxyInfo<ManagerProtocol> proxyInfo = null;

        proxyInfo = ManisDbProxies.createProxy(conf, manisDbUri, ManagerProtocol.class);
        this.manisDb = proxyInfo.getProxy();
    }

    public boolean setMaxTable(int tableNum) {
        return true;
    }
}
