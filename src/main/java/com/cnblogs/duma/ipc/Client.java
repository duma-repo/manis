package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.Writable;
import com.cnblogs.duma.net.NetUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import sun.nio.ch.Net;

import javax.net.SocketFactory;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author duma
 */
public class Client {
    private static final Log LOG = LogFactory.getLog(Client.class);

    /** A counter for generating call IDs. */
    private static final AtomicInteger callIdCounter = new AtomicInteger();

    /** 在当前线程保存自己的唯一的一个调用 id */
    private static final ThreadLocal<Integer> callId = new ThreadLocal<Integer>();


    private final Hashtable<ConnectionId, Connection> connections = new Hashtable<>();
    private Class<? extends Writable> valueClass;
    private AtomicBoolean running = new AtomicBoolean(true);
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

    Call createCall(RPC.RpcKind rpcKind, Writable rpcRequest) {
        return new Call(rpcKind, rpcRequest);
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

            Integer id = callId.get();
            if (id == null) {
                // 返回 null 说明可以取一个新的自增id
                this.id = nextCallId();
            } else {
                // 返回非null，可能是重试，重试的情况不需要获取新的id
                callId.set(null);
                this.id = id;
            }
            //todo retry
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
        final Call call = createCall(rpcKind, rpcRequest);
        Connection connection = getConnection(remoteId, call, serviceClass);
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
        private int pingInterval;
        /** socket 连接超时最大重试次数 */
        private final int maxRetriesOnSocketTimeouts;
        private int serviceClass;

        /** 标识是否应该关闭连接，默认值： false */
        private AtomicBoolean shouldCloaseConnection = new AtomicBoolean();
        private Hashtable<Integer, Call> calls = new Hashtable<>();
        /** I/O 活动的最新时间 */
        private AtomicLong lastActivity = new AtomicLong();


        public Connection(ConnectionId remoteId, Integer serviceClass) throws IOException {
            this.remoteId = remoteId;
            this.server = remoteId.getAddress();
            if (server.isUnresolved()) {
                throw new UnknownHostException("Unknown host name : " + server.toString());
            }
            this.rpcTimeOut = remoteId.getRpcTimeOut();
            this.maxRetriesOnSocketTimeouts = remoteId.getMaxRetriesOnSocketTimeouts();
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

        /**
         * 将当前时间更新为 I/O 最新活动时间
         */
        private void touch() {
            lastActivity.set(System.currentTimeMillis());
        }

        /**
         * 向该 Connection 对象的 call 队列加入一个 call
         * 同时唤醒等待的线程
         * @param call 加入的队列得元素
         * @return 如果连接处于关闭状态是添加 call，返回 false。正确加入队列返回 true
         */
        private synchronized boolean addCall(Call call) {
            if (shouldCloaseConnection.get()) {
                return false;
            }
            calls.put(call.id, call);
            notify();
            return true;
        }

        private synchronized void setupConnection() throws IOException {
            short ioFailures = 0;
            short timeOutFailures = 0;
            while (true) {
                try {
                    this.socket = socketFactory.createSocket();
                    this.socket.setTcpNoDelay(tcpNoDelay);
                    this.socket.setKeepAlive(true);

                    NetUtils.connect(socket, server, connectionTimeOut);

                    if (rpcTimeOut > 0) {
                        // 用 rpcTimeOut 覆盖 pingInterval
                        pingInterval = rpcTimeOut;
                    }
                    socket.setSoTimeout(pingInterval);
                    return;
                } catch (SocketTimeoutException ste) {
                    handleConnectionTimeout(timeOutFailures++, maxRetriesOnSocketTimeouts, ste);
                } catch (IOException ioe) {
                    //todo retry
                    throw ioe;
                }
            }
        }

        /**
         * 连接 server，建立 IO 流。向 server 发送 header 信息
         * 启动线程，等待返回信息。由于多个线程持有相同 Connection 对象，需要保证只有一个线程可以调用 start 方法
         * 因此该方法需要用 synchronized 修饰
         */
        private synchronized void setupIOStreams() {
            if (socket != null || shouldCloaseConnection.get()) {
                return;
            }

            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Connecting to " + server);
                }
                setupConnection();
                InputStream inStream = NetUtils.getInputStream(socket);
                OutputStream outStream = NetUtils.getOutputStream(socket);
                writeConnectionHeader(outStream);

                if (doPing) {
                    /**
                     * todo ping相关
                     */
                }

                /**
                 * DataInputStream 可以支持 Java 原子类型的输入输入
                 * BufferedInputStream 具有缓冲的作用
                 */
                this.in = new DataInputStream(new BufferedInputStream(inStream));
                this.out = new DataOutputStream(new BufferedOutputStream(outStream));

//                writeConnectionContext todo 写连接的上下文

                touch();

                // 建立连接，发送完连接头和连接上线文后，启动 receiver 线程，用来接收响应信息
                start();
                return;
            } catch (Throwable e) {
                //todo 异常处理
                LOG.info(e);
            }
        }

        /**
         * 建立连接后发送的连接头（header）
         * +----------------------------------+
         * |  "mrpc" 4 bytes                  |
         * +----------------------------------+
         * |  Version (1 byte)                |
         * +----------------------------------+
         * |  Service Class (1 byte)          |
         * +----------------------------------+
         * |  AuthProtocol (1 byte)           |
         * +----------------------------------+
         * @param outStream 输出流
         * @throws IOException
         */
        private void writeConnectionHeader(OutputStream outStream) throws IOException {
           DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream));

           out.write(RpcConstants.HEADER.array());
           out.write(RpcConstants.CURRENT_VERSION);
           out.write(serviceClass);
           // 暂无授权协议，写 0
           out.write(0);
           out.flush();
        }

        private void closeConnection() {
            if (socket == null) {
                return;
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
                LOG.warn("Not able to close a socket", e);
            }
            //将 socket 置位 null 为了下次能够重新建立连接
            socket = null;
        }

        private void handleConnectionTimeout(
                int curRetries, int maxRetries, IOException ioe) throws IOException {

            closeConnection();

            /**
             * 达到重试的最大次数，将异常抛出
             */
            if (curRetries >= maxRetries) {
                throw ioe;
            }
            LOG.info("Retrying connect to server: " + server + ". Already tried "
                    + curRetries + " time(s); maxRetries=" + maxRetries);
        }

        @Override
        public void run() {
            super.run();
        }
    }

    /**
     * 从缓冲池中获取一个 Connection 对象，如果池中不存在，需要创建对象并放入缓冲池
     *
     * @return Connection 对象
     */
    private Connection getConnection(ConnectionId remoteId,
                                     Call call, int serviceClass) throws IOException {
        if (!running.get()) {
            throw new IOException("The client is stopped.");
        }
        Connection connection;
        do {
            synchronized (connections) {
               connection = connections.get(remoteId);
               if (connection == null) {
                   connection = new Connection(remoteId, serviceClass);
                   connections.put(remoteId, connection);
               }
            }
        } while (!connection.addCall(call));

        //我们没有在上面 synchronized (connections) 代码块调用该方法
        //原因是如果服务端慢，建立连接会花费很长时间，会拖慢整个系统
        connection.setupIOStreams();
        return null;
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
        /** socket 连接超时最大重试次数 */
        private final int maxRetriesOnSocketTimeouts;
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
            this.maxRetriesOnSocketTimeouts = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
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

        public int getMaxRetriesOnSocketTimeouts() {
            return maxRetriesOnSocketTimeouts;
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
