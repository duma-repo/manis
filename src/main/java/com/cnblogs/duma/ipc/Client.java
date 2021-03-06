package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.IOUtils;
import com.cnblogs.duma.io.Writable;
import com.cnblogs.duma.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper;
import com.cnblogs.duma.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.*;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import com.cnblogs.duma.net.NetUtils;
import com.cnblogs.duma.util.ProtoUtil;
import com.cnblogs.duma.util.StringUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.CodedOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
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
    private final byte[] clientId;

    /**
     * 用来发送 调用header、方法名和参数信息
     * Executor 可以提供线程池
     * todo 需要修改
     */
    private final ExecutorService sendParamsExecutor;
    private final ClientExecutorServiceFactory clientExecutorFactory =
            new ClientExecutorServiceFactory();

    private static class ClientExecutorServiceFactory {
        private int executorRefCount = 0;
        private ExecutorService clientExecutor = null;

        /**
         * 获得 Executor 用来发送 rpc 调用参数
         * 如果内部引用计数器（executorRefCount）为 0，初始化
         * 否则直接返回 Executor
         * 为了保证唯一性，调用该函数时需要加锁
         * @return ExecutorService 实例
         */
        synchronized ExecutorService refAndGetInstance() {
            if (executorRefCount == 0) {
                clientExecutor = Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setDaemon(true)
                                .setNameFormat("IPC Parameter Sending Thread #%d")
                                .build());
            }
            executorRefCount++;
            return clientExecutor;
        }

        synchronized void unrefAndCleanup() {
            executorRefCount--;
            assert executorRefCount >= 0;

            if (executorRefCount == 0) {
                clientExecutor.shutdown();
                /**
                 * 一分钟后如果仍然没有关闭或者在等待过程中被中断，
                 * 则调用 {@link ExecutorService#shutdownNow()}
                 */
                try {
                    if (!clientExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                        clientExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while waiting for clientExecutor" +
                            "to stop", e);
                    clientExecutor.shutdownNow();
                }

                clientExecutor = null;
            }
        }
    }

    public Client(Class<? extends Writable> valueClass, Configuration conf,
                  SocketFactory factory) {
        this.valueClass = valueClass;
        this.conf = conf;
        this.socketFactory = factory;
        this.connectionTimeOut = conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);
        this.clientId = ClientId.getClientId();
        this.sendParamsExecutor = clientExecutorFactory.refAndGetInstance();
    }

    public void stop() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Stopping client");
        }

        if (!running.compareAndSet(true, false)) {
            return;
        }

        // 唤醒 connection
        synchronized (connections) {
            for (Connection connection : connections.values()) {
                connection.interrupt();
            }
        }

        // 等待，直到所有connection关闭
        while (!connections.isEmpty()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        clientExecutorFactory.unrefAndCleanup();
    }

    void checkResponse(RpcResponseHeaderProto header) throws IOException {
        if (header == null) {
            throw new IOException("Response is null.");
        }

        if (header.hasClientId()) {
            final byte[] responseId = header.getClientId().toByteArray();
            if (!Arrays.equals(responseId, RpcConstants.DUMMY_CLIENT_ID)) {
                if (!Arrays.equals(responseId, clientId)) {
                    throw new IOException("Client IDs not matched: local ID="
                            + StringUtils.byteToHexString(clientId) + ", ID in response="
                            + StringUtils.byteToHexString(header.getClientId().toByteArray()));
                }
            }
        }
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
        try {
            // 发送 rpc 请求
            connection.sendRpcRequest(call);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("interrupted waiting to send rpc request to server", e);
            throw new IOException(e);
        }

        boolean interrupted = false;
        // 为了能在 call 上调用 wait 方法，需要在 call 对象上加锁
        synchronized (call) {
            while (!call.done) {
                try {
                    call.wait();
                } catch (InterruptedException e) {
                    // 保存被中断过的标记
                    interrupted = true;
                }
            }

            if (interrupted) {
                // 结束等待并且被中断过， 需要设置中断
                Thread.currentThread().interrupt();
            }

            if (call.error != null) {
                if (call.error instanceof RemoteException) {
                    call.error.fillInStackTrace();
                    throw call.error;
                } else { // 本地异常
                    InetSocketAddress address = connection.getServer();
                    Class<? extends Throwable> clazz = call.error.getClass();
                    try {
                        Constructor<? extends Throwable> ctor = clazz.getConstructor(String.class);
                        String msg = "Call From " + InetAddress.getLocalHost()
                                + " to " + address.getHostName() + ":" + address.getPort()
                                + " failed on exception: " + call.error;
                        Throwable t = ctor.newInstance(msg);
                        t.initCause(call.error);
                        throw t;
                    } catch (Throwable e) {
                        LOG.warn("Unable to construct exception of type " +
                                clazz + ": it has no (String) constructor", e);
                        throw call.error;
                    }
                }
            } else {
                return call.getRpcResponse();
            }
        }
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
        /** 关闭的原因 */
        private IOException closeException;

        private final Object sendRpcRequestLock = new Object();

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

                writeConnectionContext(remoteId);

                touch();

                // 建立连接，发送完连接头和连接上线文后，启动 receiver 线程，用来接收响应信息
                start();
                return;
            } catch (Throwable t) {
                if (t instanceof IOException) {
                    markClosed((IOException) t);
                } else {
                    markClosed(new IOException("Couldn't set up IO streams", t));
                }
                close();
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

        /**
         * 每次连接都要写连接上下文（context）
         * @param remoteId
         */
        private void writeConnectionContext(ConnectionId remoteId) throws IOException {
            IpcConnectionContextProto connectionContext =
                    ProtoUtil.makeIpcConnectionContext(
                            RPC.getProtocolName(remoteId.getProtocol()));

            RpcRequestHeaderProto connectionContextHeader = ProtoUtil
                    .makeRpcRequestHeader(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                            RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET,
                            RpcConstants.CONNECTION_CONTEXT_CALL_ID, RpcConstants.INVALID_RETRY_COUNT,
                            clientId);

            RpcRequestMessageWrapper request =
                    new RpcRequestMessageWrapper(connectionContextHeader, connectionContext);

            out.writeInt(request.getLength());
            request.write(out);
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

        public InetSocketAddress getServer() {
            return server;
        }

        private synchronized boolean waitForWork() {
            if (calls.isEmpty() && !shouldCloaseConnection.get() && running.get()) {
                long timeout = maxIdleTime -
                        (System.currentTimeMillis() - lastActivity.get());
                try {
                    // 如果 calls 为空，先等待一段时间
                    wait(timeout);
                } catch (InterruptedException e) {
                    // 被中断属于正常现象， 无需报警
                }
            }

            if (!calls.isEmpty() && !shouldCloaseConnection.get() && running.get()) {
                return true;
            } else if (shouldCloaseConnection.get()) {
                return false;
            } else if (calls.isEmpty()) {
                // 没有了 calls ，需要终止连接
                markClosed(null);
                return false;
            } else {
                // running 状态已经为 false，但仍然有 calls
                markClosed(new IOException("", new InterruptedException()));
                return false;
            }
        }

        @Override
        public void run() {
            if (LOG.isDebugEnabled()) {
                LOG.debug(getName() + ": starting, having connections " +
                        connections.size());
            }

            try {
                while (waitForWork()) {
                    receiveRpcResponse();
                }
            } catch (Throwable t) {
                LOG.warn("Unexpected error reading responses on connection " + this, t);
                markClosed(new IOException("Error reading responses", t));
            }

            close();

            if (LOG.isDebugEnabled()) {
                LOG.debug(getName() + ": stopped, remaining connections "
                        + connections.size());
            }
        }

        /**
         * 向服务端发送 rpc 请求
         * @param call 包含 rpc 调用相关的信息
         */
        public void sendRpcRequest(final Call call)
                throws IOException, InterruptedException {
            if (shouldCloaseConnection.get()) {
                return;
            }

            /**
             * 序列化需要被发送出去的信息，这里由实际调用方法的线程来完成
             * 实际发送前各个线程可以并行地准备（序列化）待发送的信息，而不是发送线程（sendParamsExecutor）
             * 这样做的好处一方面减少锁的粒度，另一方面序列化过程中抛异常每个线程可以单独、独立地报告
             *
             * 发送的格式:
             * 0) 下面 1、2 两项的长度之和，4字节
             * 1) RpcRequestHeader
             * 2) RpcRequest
             * 1、2两项在下面代码序列化
             */
            final ByteArrayOutputStream bo = new ByteArrayOutputStream();
            final DataOutputStream tmpOut = new DataOutputStream(bo);
            // 暂时没有重试机制，因此参数 retryCount=-1
            RpcRequestHeaderProto header = ProtoUtil.makeRpcRequestHeader(
                    call.rpcKind, RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET,
                    call.id, -1, clientId);
            header.writeDelimitedTo(tmpOut);
            call.rpcRequest.write(tmpOut);

            synchronized (sendRpcRequestLock) {
                Future<?> senderFuture = sendParamsExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        //多线程并发调用服务端，需要锁住发送流 out
                        try {
                            synchronized (Connection.this.out) {
                                if (shouldCloaseConnection.get()) {
                                    return;
                                }
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug(getName() + " sending #" + call.id);
                                }

                                byte[] data = bo.toByteArray();
                                int dataLen = bo.size();
                                out.writeInt(dataLen);
                                out.write(data, 0, dataLen);
                                out.flush();
                            }
                        } catch (IOException e) {
                            /**
                             * 如果在这里发生异常，将处于不可恢复状态
                             * 因此，关闭连接，终止所有未完成的调用
                             */
                            markClosed(e);
                        } finally {
                            IOUtils.closeStream(tmpOut);
                        }
                    }
                });

                try {
                    senderFuture.get();
                } catch (ExecutionException e) {
                    // Java 有异常链，该异常可能是另一个异常引起的
                    // 调用 getCause 方法获得真正的异常
                    Throwable cause = e.getCause();

                    /**
                     * 这里只能是运行时异常，因为 IOException 异常以及在上面的匿名内部类捕获了
                     */
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    } else {
                        throw new RuntimeException("unexpected checked exception", cause);
                    }
                }
            }
        }

        private void receiveRpcResponse() {
            if (shouldCloaseConnection.get()) {
                return;
            }
            touch();

            try {
                int totalLen = in.read();
                RpcResponseHeaderProto header =
                        RpcResponseHeaderProto.parseDelimitedFrom(in);
                checkResponse(header);

                int headerLen = header.getSerializedSize();
                headerLen += CodedOutputStream.computeRawVarint32Size(headerLen);

                int callId = header.getCallId();
                if (LOG.isDebugEnabled())
                    LOG.debug(getName() + " got value #" + callId);

                Call call = calls.get(callId);
                RpcStatusProto status = header.getStatus();
                if (status == RpcStatusProto.SUCCESS) {
                    Writable value = null;
                    try {
                        /** todo 封装一个方法 */
                        Constructor<? extends Writable> constructor =
                                valueClass.getDeclaredConstructor();
                        constructor.setAccessible(true);
                        value = constructor.newInstance();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    value.readFields(in);
                    calls.remove(callId);
                    call.setRpcResponse(value);
                } else {
                    if (totalLen != headerLen) {
                        throw new RpcClientException(
                                "RPC response length mismatch on rpc error");
                    }

                    String exceptionClassName =
                            header.hasExceptionClassName() ?
                                    header.getExceptionClassName() : "ServerDidNotSetExceptionClassName";
                    String errMsg =
                            header.hasErrorMsg() ?
                                    header.getErrorMsg() : "ServerDidNotSetErrorMsg";
                    final RpcErrorCodeProto errCode =
                            header.hasErrorDetail() ?
                                    header.getErrorDetail() : null;
                    if (errCode == null) {
                        LOG.warn("Detailed error code not set by server on rpc error");
                    }
                    RemoteException re =
                            new RemoteException(exceptionClassName, errMsg, errCode);
                    if (status == RpcStatusProto.ERROR) {
                        calls.remove(callId);
                        call.setException(re);
                    } else {
                        markClosed(re);
                    }
                }
            } catch (IOException e) {
                markClosed(e);
            }
        }

        private synchronized void markClosed(IOException e) {
            if (shouldCloaseConnection.compareAndSet(false, true)) {
                closeException = e;
                notifyAll();
            }
        }

        /**
         * 关闭连接
         */
        private synchronized void close() {
            if (!shouldCloaseConnection.get()) {
                LOG.error("The connection is not in the closed state");
                return;
            }

            // 释放连接资源
            synchronized (connections) {
                if (connections.get(remoteId) == this) {
                    connections.remove(remoteId);
                }
            }

            IOUtils.closeStream(in);
            IOUtils.closeStream(out);

            if (closeException == null) {
                if (!calls.isEmpty()) {
                    LOG.warn("A connection is closed for no cause and calls are not empty");
                    closeException = new IOException("Unexpected closed connection");
                    cleanupCalls();
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("closing ipc connection to " + server + ": " +
                            closeException.getMessage(),closeException);
                }
                cleanupCalls();
            }
            closeConnection();
            if (LOG.isDebugEnabled())
                LOG.debug(getName() + ": closed");
        }

        /**
         * 清楚所有的 call，并标记为异常
         */
        private void cleanupCalls() {
            Iterator<Map.Entry<Integer, Call>> iter = calls.entrySet().iterator();
            while (iter.hasNext()) {
                Call call = iter.next().getValue();
                iter.remove();
                call.setException(closeException);
            }
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
        return connection;
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
