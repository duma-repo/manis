package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Server {
    public static final Log LOG = LogFactory.getLog(Server.class);

    protected Server(Class<?> protocol, String bindAddress, int port,
                     int numHandlers, int numReaders, int queueSizePerHandler,
                     Configuration conf) {

    }
}
