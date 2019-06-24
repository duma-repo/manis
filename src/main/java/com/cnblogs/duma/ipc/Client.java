package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {
    private final Log LOG = LogFactory.getLog(Client.class);

    /** A counter for generating call IDs. */
    private static final AtomicInteger callIdCounter = new AtomicInteger();

    /** 在当前线程保存自己的唯一的一个调用 id */
    private static final ThreadLocal<Integer> callId = new ThreadLocal<Integer>();


    private Class<? extends Writable> valueClass;
    final private Configuration conf;
    /** 创建 socket 的方式 */
    private SocketFactory socketFactory;
    private final int connectionTimeOut;

    public Client(Class<? extends Writable> valueClass, Configuration conf,
                  SocketFactory factory) {
        this.valueClass = valueClass;
        this.conf = conf;
        this.socketFactory = factory;
        this.connectionTimeOut = conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);
    }

    /**
     * 代表 rpc 调用的类
     */
    static class Call {
        final int id;
        Writable rpcRequest;
        Writable rpcResponse;
        IOException error;
        final RPC.RpcKind rpcKind;
        boolean done;

        private Call(RPC.RpcKind rpcKind, Writable rpcRequest) {
            this.rpcKind = rpcKind;
            this.rpcRequest = rpcRequest;

            // todo get id
            Integer id = callId.get();
            if (id == null) {
                // 返回 null 说明可以取一个新的自增id
                this.id = nextCallId();
            } else {
                // 返回非null，可能是重试，重试的情况不需要获取新的id
                callId.set(null);
                this.id = id;
            }
            //todo get id
        }

        /**
         * 调用完成，唤醒调用者
         */
        public synchronized void callComplete() {
            this.done = true;
            notify();
        }

        /**
         * 调用过程发生错误是，设置异常信息
         * @param error 异常信息
         */
        public synchronized void setException(IOException error) {
            this.error = error;
            callComplete();
        }

        /**
         * 设置返回值.
         * Notify the caller the call is done.
         * todo synchronized
         * @param rpcResponse 调用的返回值
         */
        public synchronized void setRpcResponse(Writable rpcResponse) {
            this.rpcResponse = rpcResponse;
            callComplete();
        }

        public synchronized Writable getRpcResponse() {
            return rpcResponse;
        }
    }

    /**
     * 返回自增的 id，由于存在线程安全问题，因此 counter 是 atomic 类型
     * 为了防止 id 是取负值，需要将返回结果与 0x7FFFFFFF 做按位与操作，
     * 因此 id 取值范围是 [ 0, 2^31 - 1 ]，当 id 达到最大值，会重新从 0 开始自增
     *
     * @return 下一个自增的 id
     */
    public static int nextCallId() {
        return callIdCounter.getAndIncrement() & 0x7FFFFFFF;
    }

    /**
     * 调用 RPC 服务端，相关信息定义在 <code>remoteId</code>
     *
     * @param rpcKind - rpc 类型
     * @param rpcRequest -  包含序列化方法和参数
     * @param remoteId - rpc server
     * @returns rpc 返回值
     * 抛网络异常或者远程代码执行异常
     */
    public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest, ConnectionId remoteId)
        throws IOException {
        return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT);
    }

    /**
     * 调用 RPC 服务端，相关信息定义在 <code>remoteId</code>
     * @param rpcKind - rpc 类型
     * @param rpcRequest - 包含序列化方法和参数
     * @param remoteId - rpc server
     * @param serviceClass service class for rpc
     * @return rpc 返回值
     * @throws IOException 抛网络异常或者远程代码执行异常
     */
    public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
                         ConnectionId remoteId, int serviceClass)
            throws IOException {
        return null;
    }

    private class Connection extends Thread {
        private final ConnectionId remoteId;
        private InetSocketAddress server;

        private Socket socket = null;
        private DataInputStream in;
        private DataOutputStream out;

        private final int rpcTimeOut;
        /** 连接空闲时最大休眠时间，单位：毫秒 */
        private final int maxIdleTime;
        /** 如果 true 表示禁用 Nagle 算法 */
        private final boolean tcpNoDelay;
        /** 是否需要发送ping message */
        private final boolean doPing;
        /** 发送 ping message 的时间间隔， 单位：毫秒 */
        private final int pingInterval;
        private int serviceClass;


        public Connection(ConnectionId remoteId, int serviceClass) throws IOException {
            this.remoteId = remoteId;
            this.server = remoteId.getAddress();
            if (server.isUnresolved()) {
                throw new UnknownHostException("Unknown host name : " + server.toString());
            }
            this.rpcTimeOut = remoteId.getRpcTimeOut();
            this.maxIdleTime = remoteId.getMaxIdleTime();
            this.tcpNoDelay = remoteId.isTcpNoDelay();
            this.doPing = remoteId.isDoPing();
            if (doPing) {
                // 构造 RPC header with the callId as the ping callId todo ping message
            }
            this.pingInterval = remoteId.getPingInterval();
            this.serviceClass = serviceClass;
            if (LOG.isDebugEnabled()) {
                LOG.debug("The ping interval is " + this.pingInterval + " ms.");
            }

            this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
                    server.toString());
            this.setDaemon(true);
        }

        @Override
        public void run() {
            super.run();
        }
    }

    /**
     *  该类用来存储与连接相关的 address、protocol 等信息
     *  todo 1. 需要 getConnectionId
     *      2. 重试机制
     *      3. 解释hashcode
     */
    public static class ConnectionId {
        final InetSocketAddress address;
        private static final int PRIME = 16777619;
        private final Class<?> protocol;
        private final int rpcTimeOut;
        /** 连接空闲时最大休眠时间，单位：毫秒 */
        private final int maxIdleTime;
        /** 如果 true 表示禁用 Nagle 算法 */
        private final boolean tcpNoDelay;
        /** 是否需要发送ping message */
        private final boolean doPing;
        /** 发送 ping message 的时间间隔， 单位：毫秒 */
        private final int pingInterval;
        private final Configuration conf;

        public ConnectionId(InetSocketAddress address,
                            Class<?> protocol,
                            int rpcTimeOut,
                            Configuration conf) {
            this.address = address;
            this.protocol = protocol;
            this.rpcTimeOut = rpcTimeOut;

            this.maxIdleTime = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
            this.tcpNoDelay = conf.getBoolean(
                    CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT);
            this.doPing = conf.getBoolean(
                    CommonConfigurationKeysPublic.IPC_CLIENT_PING_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_PING_DEFAULT);
            this.pingInterval = doPing ?
                    conf.getInt(
                            CommonConfigurationKeysPublic.IPC_PING_INTERVAL_KEY,
                            CommonConfigurationKeysPublic.IPC_PING_INTERVAL_DEFAULT)
                    : 0;
            this.conf = conf;
        }

        public InetSocketAddress getAddress() {
            return address;
        }

        public Class<?> getProtocol() {
            return protocol;
        }

        public int getRpcTimeOut() {
            return rpcTimeOut;
        }

        public int getMaxIdleTime() {
            return maxIdleTime;
        }

        public boolean isTcpNoDelay() {
            return tcpNoDelay;
        }

        public boolean isDoPing() {
            return doPing;
        }

        public int getPingInterval() {
            return pingInterval;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof ConnectionId) {
                ConnectionId that = (ConnectionId) obj;
                return Objects.equals(this.address, that.address)
                        && Objects.equals(this.protocol, that.protocol)
                        && this.rpcTimeOut == that.rpcTimeOut
                        && this.maxIdleTime == that.maxIdleTime
                        && this.tcpNoDelay == that.tcpNoDelay
                        && this.doPing == that.doPing
                        && this.pingInterval == that.pingInterval;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = ((address == null) ? 0 : address.hashCode());
            result = PRIME * result + (doPing ? 1231 : 1237);
            result = PRIME * result + maxIdleTime;
            result = PRIME * result + pingInterval;
            result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
            result = PRIME * result + rpcTimeOut;
            result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
            return result;
        }

        @Override
        public String toString() {
            return address.toString();
        }
    }
}
