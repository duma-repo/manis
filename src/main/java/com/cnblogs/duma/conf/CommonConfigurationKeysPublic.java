package com.cnblogs.duma.conf;

/**
 * 包含配置相关的 key
 * @author duma
 */
public class CommonConfigurationKeysPublic {
    public static final String  IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY =
            "ipc.client.connection.maxidletime";
    /** IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY 的默认值，10s */
    public static final int     IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT = 10000;

    public static final String  IPC_CLIENT_TCPNODELAY_KEY =
            "ipc.client.tcpnodelay";
    /** IPC_CLIENT_TCPNODELAY_KEY 的默认值，true */
    public static final boolean IPC_CLIENT_TCPNODELAY_DEFAULT = true;
    /** 是否允许 RPC 客户端向服务端发送 ping message */
    public static final String  IPC_CLIENT_PING_KEY = "ipc.client.ping";
    /** IPC_CLIENT_PING_KEY 的默认值，true */
    public static final boolean IPC_CLIENT_PING_DEFAULT = true;
    /** RPC 客户端向 RPC 服务端发送 ping message 的间隔 */
    public static final String  IPC_PING_INTERVAL_KEY = "ipc.ping.interval";
    /** IPC_PING_INTERVAL_KEY 的默认值，1min */
    public static final int     IPC_PING_INTERVAL_DEFAULT = 60000;
    /** RPC连接超时 */
    public static final String  IPC_CLIENT_CONNECT_TIMEOUT_KEY =
            "ipc.client.connect.timeout";
    /** IPC_CLIENT_CONNECT_TIMEOUT_KEY 的默认值，20s */
    public static final int     IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT = 20000;
}
