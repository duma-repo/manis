package com.cnblogs.duma;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.protocol.ClientProtocol;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

/**
 *
 * @author duma
 */
public class ManisClient {
    final ClientProtocol manisDb;

    public ManisClient(URI manisDbUri, Configuration conf) throws IOException {
        ManisDbProxies.ProxyInfo<ClientProtocol> proxyInfo = null;

        proxyInfo = ManisDbProxies.createProxy(conf, manisDbUri, ClientProtocol.class);
        this.manisDb = proxyInfo.getProxy();
    }

    /**
     * 获取远程数据库表中的记录数
     * @param dbName 数据库名称
     * @param tbName 表名称
     * @return 表记录数
     * @see com.cnblogs.duma.protocol.ClientProtocol#getTableCount(String, String)
     */
    public int getTableCount(String dbName, String tbName)
            throws IOException {
        return this.manisDb.getTableCount(dbName, tbName);
    }

}
