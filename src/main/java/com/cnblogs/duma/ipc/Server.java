package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.Writable;
import com.cnblogs.duma.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.*;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import com.cnblogs.duma.util.ProtoUtil;
import com.cnblogs.duma.util.StringUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author duma
 */
public abstract class Server {

    static class RpcKindMapValue {
        final Class<? extends Writable> rpcRequestWrapperClass;
        final RPC.RpcInvoker rpcInvoker;

        RpcKindMapValue(Class<? extends Writable> rpcRequestWrapperClass,
                        RPC.RpcInvoker rpcInvoker) {
            this.rpcRequestWrapperClass = rpcRequestWrapperClass;
            this.rpcInvoker = rpcInvoker;
        }
    }
    static Map<RPC.RpcKind, RpcKindMapValue> rpcKindMap = new HashMap<>();

    /**
     * 在 rpcKind 上注册用来反序列化的类和进行rpc调用的对象
     * @param rpcKind rpcKind
     * @param rpcRequestWrapperClass 反序列化的类
     * @param rpcInvoker 用来调用的对象
     */
    static void registerProtocolEngine(RPC.RpcKind rpcKind,
                                       Class<? extends Writable> rpcRequestWrapperClass,
                                       RPC.RpcInvoker rpcInvoker) {
        RpcKindMapValue rpcKindMapValue =
                new RpcKindMapValue(rpcRequestWrapperClass, rpcInvoker);
        RpcKindMapValue old = rpcKindMap.put(rpcKind, rpcKindMapValue);
        if (old != null) {
            rpcKindMap.put(rpcKind, old);
            throw new IllegalArgumentException("ReRegistration of rpcKind: " +  rpcKind);
        }
        LOG.debug("rpcKind=" + rpcKind +
                ", rpcRequestWrapperClass=" + rpcRequestWrapperClass +
                ", rpcInvoker=" + rpcInvoker);
    }

    Class<? extends Writable> getRpcRequestWrapper(
            RpcHeaderProtos.RpcKindProto rpcKind) {
        RpcKindMapValue val = rpcKindMap.get(ProtoUtil.converRpcKind(rpcKind));
        return (val == null) ? null : val.rpcRequestWrapperClass;
    }

    public static RPC.RpcInvoker getRpcInvoker(RPC.RpcKind rpcKind) {
        RpcKindMapValue val = rpcKindMap.get(rpcKind);
        return (val == null) ? null : val.rpcInvoker;
    }

    private static final ThreadLocal<Call> curCall = new ThreadLocal<>();

    public static final Log LOG = LogFactory.getLog(Server.class);
    private String bindAddress;
    private int port;
    private int handlerCount;
    private int readThreads;
    private int readerPendingConnectionQueue;

    private int maxQueueSize;
    private int maxRespSize;
    private int maxDataLength;
    private boolean tcpNoDelay;


    volatile private boolean running = true; // todo 为什么volatile
    /** 默认使用 LinkedBlockingQueue */
    private BlockingQueue<Call> callQueue;

    private ConnectionManager connectionManager;
    private Listener listener;
    private Responder responder;
    private Handler[] handlers = null;

    private Configuration conf;

    protected Server(String bindAddress, int port,
                     int numHandlers, int numReaders, int queueSizePerHandler,
                     Configuration conf) throws IOException {
        this.conf = conf;
        this.bindAddress = bindAddress;
        this.port = port;
        this.handlerCount = numHandlers;

        if (numReaders != -1) {
            this.readThreads = numReaders;
        } else {
            this.readThreads = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_THREADS_KEY,
                    CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
        }
        if (queueSizePerHandler != -1) {
            this.maxQueueSize = numHandlers * queueSizePerHandler;
        } else {
            this.maxQueueSize = numHandlers * conf.getInt(
                    CommonConfigurationKeysPublic.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
                    CommonConfigurationKeysPublic.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);
        }
        this.maxDataLength = conf.getInt(
                CommonConfigurationKeysPublic.IPC_MAXIMUM_DATA_LENGTH,
                CommonConfigurationKeysPublic.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
        this.maxRespSize = conf.getInt(
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);
        this.readerPendingConnectionQueue = conf.getInt(
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY,
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_DEFAULT);
        this.callQueue = new LinkedBlockingDeque<>(maxQueueSize);

        // 创建 listener
        this.listener = new Listener();
        this.connectionManager = new ConnectionManager();
        // todo 连接相关的对象
        this.tcpNoDelay = conf.getBoolean(
                CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT);

        // 创建 responder
        this.responder = new Responder();
    }

    /**
     * 为 IPC 调用构造响应信息
     * @param responseBuf 序列化响应信息的 buffer
     * @param call {@link Call} 对象
     * @param status 调用状态
     * @param errorCode 调用错误码
     * @param resValue 如果调用成功，代表调用的返回值。如果调用失败则为 null
     * @param errorClass error class
     * @param error 调用错误的堆栈信息
     * @throws IOException
     */
    private void setupResponse(ByteArrayOutputStream responseBuf,
                               Call call, RpcStatusProto status, RpcErrorCodeProto errorCode,
                               Writable resValue, String errorClass, String error) throws IOException {
        responseBuf.reset();
        DataOutputStream out = new DataOutputStream(responseBuf);
        RpcResponseHeaderProto.Builder headerBuilder =
                RpcResponseHeaderProto.newBuilder();
        headerBuilder.setClientId(ByteString.copyFrom(call.clientId));
        headerBuilder.setCallId(call.callId);
        headerBuilder.setRetryCount(call.retryCount);
        headerBuilder.setStatus(status);
        headerBuilder.setServerIpcVersionNum(RpcConstants.CURRENT_VERSION);

        if (status == RpcStatusProto.SUCCESS) {
            RpcResponseHeaderProto header = headerBuilder.build();
            final int headerLen = header.getSerializedSize();
            int fullLength = CodedOutputStream.computeRawVarint32Size(headerLen) + headerLen;
            try {
                ByteArrayOutputStream bo = new ByteArrayOutputStream();
                DataOutputStream tmpOut = new DataOutputStream(bo);
                resValue.write(tmpOut);
                byte[] data = bo.toByteArray();

                fullLength += data.length;
                out.write(fullLength);
                header.writeDelimitedTo(out);
                out.write(data, 0, data.length);
            } catch (Throwable t) {
                LOG.warn("Error serializing call response for call " + call, t);
                // 序列化出错时，需要递归调用该函数，创建一个序列化错误的响应信息
                setupResponse(responseBuf, call,
                        RpcStatusProto.ERROR, RpcErrorCodeProto.ERROR_SERIALIZING_RESPONSE,
                        null, t.getClass().getName(),
                        StringUtils.stringifyException(t));
            }
        } else {
            headerBuilder.setErrorDetail(errorCode);
            headerBuilder.setExceptionClassName(errorClass);
            headerBuilder.setErrorMsg(error);
            RpcResponseHeaderProto header = headerBuilder.build();
            int headerLen = header.getSerializedSize();
            final int fullLength = CodedOutputStream.computeRawVarint32Size(headerLen) + headerLen;
            out.write(fullLength);
            header.writeDelimitedTo(out);
        }
        call.setResponse(ByteBuffer.wrap(responseBuf.toByteArray()));
    }

    /**
     * 启动服务
     * todo synchronized 必须加的吗
     */
    public synchronized void start() {
        listener.start();
        responder.start();
        handlers = new Handler[handlerCount];

        for (int i = 0; i < handlerCount; i++) {
            handlers[i] = new Handler(i);
            handlers[i].start();
        }
    }

    /**
     * 等待服务端停止
     * @throws InterruptedException
     */
    public synchronized void join() throws InterruptedException {
        while (running) {
            wait();
        }
    }

    public abstract Writable call(RPC.RpcKind rpcKind, String protocol,
                                  Writable param, long receiveTime) throws Exception;

    private void closeConnection(Connection conn) {
        connectionManager.close(conn);
    }
    /**
     * 封装 socket 与 address 绑定的代码，为了在这层做异常的处理
     * @param socket 服务端 socket
     * @param address 需要绑定的地址
     * @throws IOException
     */
    public static void bind(ServerSocket socket, InetSocketAddress address,
                            int backlog) throws IOException {
        try {
            socket.bind(address, backlog);
        } catch (SocketException se) {
            throw new IOException("Failed on local exception: "
                    + se
                    + "; bind address: " + address, se);
        }
    }

    /**
     * 当读写 buffer 的大小超过 64KB 限制，读写操作的数据将按照该值分割。
     * 大部分 RPC 不会拆过这个值
     */
    private static int NIO_BUFFER_LIMIT = 8*1024;

    /**
     * 该函数是 {@link WritableByteChannel#write(ByteBuffer)} 的 wrapper
     * 如果需要读数据量较大, 可以分成小块从 channel 写数据。这样，可以避免
     * 随着 ByteBuffer 大小的增加，jdk 创建大量 buffer，从而避免性能衰减
     *
     * @see WritableByteChannel#write(ByteBuffer)
     * @param channel
     * @param buffer
     * @return 写入的字节数
     * @throws IOException
     */
    private int channelWrite(WritableByteChannel channel,
                             ByteBuffer buffer) throws IOException {
        int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.write(buffer) : channelIO(null, channel, buffer);
        return count;
    }

    /**
     * 该函数是 {@link ReadableByteChannel#read(ByteBuffer)} 的 wrapper
     * 如果需要读数据量较大, 可以分成小块从 channel 读数据。这样，可以避免
     * 随着 ByteBuffer 大小的增加，jdk 创建大量 buffer，从而避免性能衰减
     *
     * @see ReadableByteChannel#read(ByteBuffer)
     */
    private int channelRead(ReadableByteChannel channel,
                            ByteBuffer buffer) throws IOException {
        int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.read(buffer) : channelIO(channel, null, buffer);
        return count;
    }

    private int channelIO(ReadableByteChannel readCh,
                          WritableByteChannel writeCh,
                          ByteBuffer buffer) {
        return 0;
    }

    public static class Call {
        private final int callId;
        private final int retryCount;
        /** 客户端发来的序列化的 RPC 请求 */
        private final Writable rpcRequest;
        private final Connection connection;
        private long timestamp;

        /** 本次调用的响应信息 */
        private ByteBuffer response;
        private final RPC.RpcKind rpcKind;
        private final byte[] clientId;

        public Call(int id, int retryCount, Writable rpcRequest, Connection connection) {
            this(id, retryCount, rpcRequest, connection,
                    RPC.RpcKind.RPC_BUILTIN, RpcConstants.DUMMY_CLIENT_ID);
        }

        public Call(int id, int retryCount, Writable rpcRequest,
                    Connection connection, RPC.RpcKind rpcKind, byte[] clientId) {
            this.callId = id;
            this.retryCount = retryCount;
            this.rpcRequest = rpcRequest;
            this.connection = connection;
            this.timestamp = System.currentTimeMillis();
            this.response = null;
            this.rpcKind = rpcKind;
            this.clientId = clientId;
        }

        public void setResponse(ByteBuffer response) {
            this.response = response;
        }

        @Override
        public String toString() {
            return rpcRequest + " from " + connection + " Call#" + callId +
                    " Retry#" + retryCount;
        }
    }

    /**
     * socket 监听线程
     */
    private class Listener extends Thread {
        private ServerSocketChannel acceptChannel;
        private Selector selector;
        private Reader[] readers;
        private int currentReader = 0;
        private InetSocketAddress address;
        private int backlogLength = conf.getInt(
                CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
                CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);

        public Listener() throws IOException {
            this.address = new InetSocketAddress(bindAddress, port);
            // 创建服务端 socket，并设置为非阻塞
            this.acceptChannel = ServerSocketChannel.open();
            this.acceptChannel.configureBlocking(false);

            // 将服务端 socket 绑定到主机和端口
            bind(acceptChannel.socket(), address, backlogLength);
            // 创建 selector
            this.selector = Selector.open();
            // 注册 select ACCEPT 事件
            acceptChannel.register(selector, SelectionKey.OP_ACCEPT);

            // 创建 reader 线程并启动
            this.readers = new Reader[readThreads];
            for (int i = 0; i < readThreads; i++) {
                Reader reader = new Reader("Socket Reader #" + (i + 1) + " for port " + port);
                this.readers[i] = reader;
                reader.start();
            }

            this.setName("IPC Server listener on " + port);
            // 设置为 daemon，它的子线程也是 daemon
            this.setDaemon(true);
        }

        private class Reader extends Thread {
            private final BlockingQueue<Connection> penddingConnections;
            private final Selector readSelector;

            Reader(String name) throws IOException {
                super(name);
                this.penddingConnections =
                        new LinkedBlockingDeque<>(readerPendingConnectionQueue);
                readSelector = Selector.open();
            }

            @Override
            public void run() {
                LOG.info("Starting " + Thread.currentThread().getName());
                try {
                    doRunLoop();
                } finally {
                    try {
                        readSelector.close();
                    } catch (IOException ioe) {
                        LOG.error("Error closing read selector in " + Thread.currentThread().getName(), ioe);
                    }
                }
            }

            void doRunLoop() {
                while (running) {
                    SelectionKey key = null;
                    try {
                        // 消费已进队列的 connection 的数量，防止队列为空是阻塞等待
                        // 影响后续 select
                        int size = penddingConnections.size();
                        for (int i = 0; i < size; i++) {
                            Connection conn = penddingConnections.take();
                            conn.channel.register(readSelector, SelectionKey.OP_READ, conn);
                        }
                        readSelector.select();

                        Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
                        while (iter.hasNext()) {
                            key = iter.next();
                            iter.remove();
                            if (key.isValid() &&
                                key.isReadable()) {
                                doRead(key);
                            }
                            key = null;
                        }
                    } catch (InterruptedException e) {
                        LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
                    } catch (IOException e) {
                        LOG.error("Error in Reader", e);
                    }
                }
            }

            /**
             * 生产者将 connection 入队列
             * 唤醒 readSelector，执行下一次读操作之前会为新的 connection 注册读事件
             * @param conn 新的 Connection 对象
             * @throws InterruptedException
             */
            void addConnection(Connection conn) throws InterruptedException {
                penddingConnections.put(conn);
                readSelector.wakeup();
            }
        }

        @Override
        public void run() {
            LOG.info("Listener thread " + Thread.currentThread().getName() + ": starting");
            connectionManager.startIdleScan();
            while (running) {
                SelectionKey key = null;
                try {
                    selector.select();
                    Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                    while (iterator.hasNext()) {
                        key = iterator.next();
                        iterator.remove();
                        if (key.isValid()) {
                            if (key.isAcceptable()) {
                                System.out.println("client accept");
                                doAccept(key);
                            }
                        }
                        key = null;
                    }
                } catch (OutOfMemoryError oom) {
                    // out of memory 时，需要关闭所有连接
                    // todo 休眠一段时间等其他线程处理后事
                    LOG.warn("Out of Memory in server select", oom);
                    closeCurrentConnection(key);
                    connectionManager.closeIdle(true);
                    try { Thread.sleep(60000); } catch (InterruptedException e) {}
                } catch (Exception e) {
                    closeCurrentConnection(key);
                }
            }
            // 需要停止，退出循环
            LOG.info("Stopping " + Thread.currentThread().getName());

            try {
                acceptChannel.close();
                selector.close();
            } catch (IOException e) {
                LOG.warn("Ignoring listener close exception", e);
            }

            acceptChannel = null;
            selector = null;

            connectionManager.stopIdleScan();
            connectionManager.closeAll();
        }

        private void closeCurrentConnection(SelectionKey key) {
            if (key != null) {
                Connection conn = (Connection) key.attachment();
                if (conn != null) {
                    closeConnection(conn);
                    conn = null;
                }
            }
        }

        void doAccept(SelectionKey key) throws IOException, InterruptedException {
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel channel;
            while ((channel = server.accept()) != null) {
                channel.configureBlocking(false);
                channel.socket().setTcpNoDelay(tcpNoDelay);
                channel.socket().setKeepAlive(true);

                Connection conn = connectionManager.register(channel);
                // 关闭当前连接时可以去到 Connection 对象
                key.attach(conn);

                Reader reader = getReader();
                reader.addConnection(conn);
            }
        }

        void doRead(SelectionKey key) {
            int count = 0;
            Connection conn = (Connection)key.attachment();
            if(conn == null) {
                return;
            }
            conn.setLastContact(System.currentTimeMillis());

            try {
                count = conn.readAndProcess();
            } catch (InterruptedException ioe) {
                LOG.info(Thread.currentThread().getName() +
                        " readAndProcess caught InterruptedException", ioe);
            } catch (Exception e) {
                // 这层进行异常的捕获，因为 WrappedRpcServerException 异常已经发送响应信息给客户端了
                // 所以这里不处理该异常
                LOG.info(Thread.currentThread().getName() + ": readAndProcess from client " +
                        conn.getHostAddress() + " throw exception [" + e + " ]",
                        (e instanceof WrappedRpcServerException) ? null : e);
                count = -1;
            }
            if (count < 0) {
                closeConnection(conn);
                conn = null;
            } else {
                conn.setLastContact(System.currentTimeMillis());
            }
        }

        Reader getReader() {
            currentReader = (currentReader + 1) % readers.length;
            return readers[currentReader];
        }
    }

    /**
     * 处理队列中的调用请求
     */
    private class Handler extends Thread {
        Handler(int instanceNumber) {
            this.setDaemon(true);
            this.setName("IPC Server handler " + instanceNumber + " on " + port);
        }

        @Override
        public void run() {
            LOG.debug(Thread.currentThread().getName() + ": starting.");
            ByteArrayOutputStream buf = new ByteArrayOutputStream(10240);
            while (running) {
                try {
                    final Call call = callQueue.take();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName() + ": " + call +
                                " for rpcKind " + call.rpcKind);
                    }
                    if (!call.connection.channel.isOpen()) {
                        LOG.info(Thread.currentThread().getName() + ": skipped " + call);
                        continue;
                    }
                    String errorClass = null;
                    String error = null;
                    RpcStatusProto returnStatus = RpcStatusProto.SUCCESS;
                    RpcErrorCodeProto detailedErr = null;
                    Writable value = null;
                    curCall.set(call);
                    try {
                        value = call(call.rpcKind, call.connection.protocolName,
                                    call.rpcRequest, call.timestamp);
                    } catch (Throwable e) {
                        String logMsg = Thread.currentThread().getName() + ", call " + call;
                        if (e instanceof RuntimeException || e instanceof Error) {
                            // 抛出该类型的错误说明服务端自身出现问题
                            LOG.warn(logMsg, e);
                        } else {
                            // 属于正常情况的异常抛出
                            LOG.info(logMsg, e);
                        }
                        if (e instanceof RpcServerException) {
                            RpcServerException rse = (RpcServerException) e;
                            returnStatus = rse.getRpcStatusProto();
                            detailedErr = rse.getRpcErrorCode();
                        } else {
                            returnStatus = RpcStatusProto.ERROR;
                            detailedErr = RpcErrorCodeProto.ERROR_APPLICATION;
                        }
                        errorClass = e.getClass().getName();
                        error = StringUtils.stringifyException(e);
                    }
                    curCall.set(null);
//                    synchronized (call.connection.) {
                    setupResponse(buf, call, returnStatus,
                            detailedErr, value, errorClass, error);
//                    }

                    //如果 buf 占用空间太大则丢弃掉，重新将 buf 调到初始大小以释放堆内存
                    if (buf.size() > maxRespSize) {
                        LOG.info("Large response size " + buf.size() + " for call "
                                + call.toString());
                        buf = new ByteArrayOutputStream(10240);
                    }
                    responder.doResponse(call);
                } catch (InterruptedException e) {
                    LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
                } catch (Exception e) {
                    LOG.info(Thread.currentThread().getName() + " caught an exception", e);
                }
            }
            LOG.debug(Thread.currentThread().getName() + ": exiting");
        }
    }

    /**
     * 向客户端发送响应结果
     */
    private class Responder extends Thread {
        private final Selector writeSelector;
        private int pending;

        final static int PURGE_INTERVAL = 900000; // 15mins

        Responder() throws IOException {
            this.setName("IPC Server Responder");
            this.setDaemon(true);
            writeSelector = Selector.open();
            pending = 0;
        }

        @Override
        public void run() {
            LOG.info(Thread.currentThread().getName() + ": starting.");
            try {
                doRunLoop();
            } finally {
                LOG.info("Stopping " + Thread.currentThread().getName());
                try {
                    writeSelector.close();
                } catch (IOException ioe) {
                    LOG.error("Couldn't close write selector in " + Thread.currentThread().getName(), ioe);
                }
            }
        }

        private void doRunLoop() {
            long lastPurgeTime = 0;

            while (running) {
                try {
                    // 如果有渠道正在注册，便先等待
                    waitPendding();
                    writeSelector.select(PURGE_INTERVAL);
                    Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        iter.remove();
                        try {
                            if (key.isValid() && key.isWritable()) {
                                doAsyncWrite(key);
                            }
                        } catch (IOException ioe) {
                            LOG.info(Thread.currentThread().getName() + ": doAsyncWrite threw exception " + ioe);
                        }
                    }
                    long now = System.currentTimeMillis();
                    if (now < lastPurgeTime + PURGE_INTERVAL) {
                        continue;
                    }
                    lastPurgeTime = now;

                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Checking for old call responses.");
                    }
                    ArrayList<Call> calls = null;

                    synchronized (writeSelector.keys()) {
                        // 锁住 writeSelector.keys() 对象，防止新的 channel 注册
                        calls = new ArrayList<>(writeSelector.keys().size());
                        iter = writeSelector.keys().iterator();
                        while (iter.hasNext()) {
                            SelectionKey key = iter.next();
                            Call call = (Call)key.attachment();
                            if (call != null && key.channel() == call.connection.channel) {
                                calls.add(call);
                            }
                        }
                    }

                    // 如果有长时间（超过15min）未发送的 calls，关闭连接，丢弃它们
                    for (Call call : calls) {
                        doPurge(call, now);
                    }
                } catch (OutOfMemoryError e) {
                    // 如果太多事件需要响应，可能会内存溢出
                    LOG.warn("Out of Memory in server select", e);
                    try { Thread.sleep(60000); } catch (Exception ie) {}
                } catch (Exception e) {
                    LOG.warn("Exception in Responder", e);
                }
            }
        }

        private void doAsyncWrite(SelectionKey key) throws IOException {
            Call call = (Call) key.attachment();
            if (call == null) {
                return;
            }
            if (call.connection.channel != key.channel()) {
                throw new IOException("doAsyncWrite: bad channel");
            }
            synchronized (call.connection.responseQueue) {
                if (processResponse(call.connection.responseQueue, false)) {
                    // 如果队列 call 处理完毕，清除 OP_WRITE
                    try {
                        key.interestOps(0);
                    } catch (CancelledKeyException e) {
                         //Listener 或者 Reader 线程可能关闭了该 socket
                        LOG.warn("Exception while changing ops : " + e);
                    }
                }
            }
        }

        /**
         * 如果 responseQueue 存在长时间未被处理的 calls，关闭该连接
         * @param call Call 对象
         * @param now
         */
        private void doPurge(Call call, long now) {
            LinkedList<Call> responseQueue = call.connection.responseQueue;
            synchronized (responseQueue) {
                ListIterator<Call> iter = responseQueue.listIterator(0);
                while (iter.hasNext()) {
                    call = iter.next();
                    if (now > call.timestamp + PURGE_INTERVAL) {
                        closeConnection(call.connection);
                        break;
                    }
                }
            }
        }

        private boolean processResponse(LinkedList<Call> responseQueue,
                                        boolean isHandler) throws IOException {
            boolean error = true;
            boolean done = false;
            int numElements = 0;
            Call call = null;

            try {
                numElements = responseQueue.size();
                if (numElements == 0) {
                    error = false;
                    return true;
                }
                // 取出第一个 Call 对象，发送响应信息
                call =  responseQueue.removeFirst();
                SocketChannel channel = call.connection.channel;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(Thread.currentThread().getName() + ": responding to " + call);
                }
                // 将响应信息发送到channel
                int numBytes = channelWrite(channel, call.response);
                if (numBytes < 0) {
                    return true;
                }

                if (!call.response.hasRemaining()) {
                    call.response = null;
                    call.connection.decRpcCount();
                    if (numElements == 1) {
                        done = true;
                    } else {
                        done = false;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName() + ": responding to " + call
                                + " Wrote " + numBytes + " bytes.");
                    }
                } else {
                    // 说明一次性没有写完，继续放入队列中
                    call.connection.responseQueue.addFirst(call);

                    /**
                     * 如果 isHandler = true 说明队列只有当前处理的一个 Call 对象，如果本次未处理完，需要重新
                     * 放入队列给该渠道注册一个 OP_WRITE 事件，后续由 Responder 线程继续处理
                     */
                    if (isHandler) {
                        call.timestamp = System.currentTimeMillis();

                        // 防止 wakeup 后 register 之前再次进入 select 等待，需要加锁，让 responder 线程等待
                        incPendding();
                        try {
                            // 如果 writeSelector 在 select 上阻塞，无法成功地 register
                            writeSelector.wakeup();
                            channel.register(writeSelector, SelectionKey.OP_WRITE, call);
                        } catch (ClosedChannelException e) {
                            done = true;
                        } finally {
                            decPendding();
                        }
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName() + ": responding to " + call
                                + " Wrote partial " + numBytes + " bytes.");
                    }
                }
                error = false;
            } finally {
                if (error && call != null) {
                    LOG.warn(Thread.currentThread().getName()+", call " + call + ": output error");
                    done = true;
                    closeConnection(call.connection);
                }
            }

            return done;
        }
        /**
         * 将 Call 对象放入 Connection 对象的响应队列中
         * 并发送响应信息
         * @param call
         */
        void doResponse(Call call) throws IOException {
            synchronized (call.connection.responseQueue) {
                call.connection.responseQueue.addLast(call);
                // 如果队列只有当前的响应信息，直接写响应信息
                // 否则，只写入队列，Handler 线程不做响应信息的发送
                if (call.connection.responseQueue.size() == 1) {
                    processResponse(call.connection.responseQueue, true);
                }
            }
        }

        private synchronized void incPendding() {
            pending++;
        }

        private synchronized void decPendding() {
            pending--;
            notify();
        }

        private synchronized void waitPendding() throws InterruptedException {
            while (pending > 0) {
                wait();
            }
        }
    }

    private static class WrappedRpcServerException extends IOException {
        private RpcErrorCodeProto errorCode;

        public WrappedRpcServerException(RpcErrorCodeProto errorCode, String message) {
            super(message, new IOException(message));
            this.errorCode = errorCode;
        }

        public RpcErrorCodeProto getErrorCode() {
            return errorCode;
        }

        @Override
        public String toString() {
            return getCause().toString();
        }
    }

    private class Connection {
        /** 连接头是否被读过 */
        private boolean connectionHeaderRead = false;
        /** 连接头后的连接上下文是否被读过 */
        private boolean connectionContextRead = false;

        private SocketChannel channel;
        private volatile long lastContact;

        private ByteBuffer data;
        private ByteBuffer dataLengthBuffer;
        private final LinkedList<Call> responseQueue;
        private Socket socket;
        private ByteBuffer connectionHeaderBuf = null;
        private InetAddress remoteAddr;
        private String hostAddress;
        private int remotePort;

        IpcConnectionContextProto connectionContext;
        String protocolName;

        public String getHostAddress() {
            return hostAddress;
        }

        private volatile int rpcCount;

        Connection(SocketChannel channel, long lastContact) {
            this.channel = channel;
            this.lastContact = lastContact;
            this.socket = channel.socket();
            this.data = null;
            this.dataLengthBuffer = ByteBuffer.allocate(4);
            this.remoteAddr = socket.getInetAddress();
            this.hostAddress = remoteAddr.getHostAddress();
            this.remotePort = socket.getPort();
            this.responseQueue = new LinkedList<Call>();
        }

        @Override
        public String toString() {
            return getHostAddress() + ":" + remotePort;
        }

        private void checkDataLength(int dataLength) throws IOException {
            if (dataLength < 0) {
                String errMsg = "Unexpected data length " + dataLength +
                        "! from " + hostAddress;
                LOG.warn(errMsg);
                throw new IOException(errMsg);
            }
            if (dataLength > maxDataLength) {
                String errMsg = "Requested data length " + dataLength +
                        " is longer than maximum configured RPC length " +
                        maxDataLength + ".  RPC came from " + hostAddress;
                LOG.warn(errMsg);
                throw new IOException(errMsg);
            }
        }

        int readAndProcess() throws IOException, InterruptedException {
            while (true) {
                // 至少读一次 RPC 的数据
                // 一直迭代直到一次 RPC 的数据读完或者没有剩余的数据
                int count = -1;
                if (dataLengthBuffer.remaining() > 0) {
                    count = channelRead(channel, dataLengthBuffer);
                    if (count < 0 || dataLengthBuffer.remaining() > 0) {
                        // 读取有问题或者未读完，直接返回
                        return count;
                    }
                }
                if (!connectionHeaderRead) {
                    if (connectionHeaderBuf == null) {
                        connectionHeaderBuf = ByteBuffer.allocate(3);
                    }
                    count = channelRead(channel, connectionHeaderBuf);
                    if (count < 0 || connectionHeaderBuf.remaining() > 0) {
                        return count;
                    }
                    int version = connectionHeaderBuf.get(0);
                    int serviceClass = connectionHeaderBuf.get(1);

                    dataLengthBuffer.flip();
                    if (!RpcConstants.HEADER.equals(dataLengthBuffer) ||
                        version != RpcConstants.CURRENT_VERSION) {
                        LOG.warn("Incorrect header or version mismatch from " +
                                hostAddress + ":" + remotePort +
                                " got version " + version +
                                " expected version " + RpcConstants.CURRENT_VERSION);
                        //todo response bad version
                        return -1;
                    }

                    dataLengthBuffer.clear();
                    connectionHeaderBuf = null;
                    connectionHeaderRead = true;
                    continue;
                }

                // 可能存在分批读的情况，所以先判断之前是否读过
                if (data == null) {
                    dataLengthBuffer.flip();
                    int dataLength = dataLengthBuffer.getInt();
                    checkDataLength(dataLength);
                    data = ByteBuffer.allocate(dataLength);
                }

                count = channelRead(channel, data);
                if (data.remaining() == 0) {
                    // 说明数据已经完全读入
                    dataLengthBuffer.clear();
                    data.flip();
                    // processOneRpc 函数可能改变 connectionContextRead 值
                    // 需要将当前值存储在临时变量中
                    boolean isHeaderRead = connectionContextRead;
                    processOneRpc(data.array());
                    data=null;
                    if (!isHeaderRead) {
                        continue;
                    }
                }
                return count;
            }
        }

        /**
         * 处理一次 RPC 请求
         * @param buf RPC 请求的上下文或者调用请求
         */
        private void processOneRpc(byte[] buf)
                throws IOException, InterruptedException {
            int callId = -1;
            int retry = RpcConstants.INVALID_RETRY_COUNT;

            try {
                DataInputStream dis =
                        new DataInputStream(new ByteArrayInputStream(buf));
                RpcRequestHeaderProto header =
                    decodeProtobufFromStream(RpcRequestHeaderProto.newBuilder(), dis);
                callId = header.getCallId();
                retry = header.getRetryCount();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("get #" + callId);
                }
                checkRpcHeader(header);
                if (callId < 0) {
                    processOutOfBandRequest(header, dis);
                } else if (!connectionContextRead) {
                    throw new WrappedRpcServerException(
                            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                            "Connection context not found");
                } else {
                    processRpcRequest(header, dis);
                }

            } catch (WrappedRpcServerException wrse) {
                Throwable ioe = wrse.getCause();
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                Call call = new Call(callId, retry, null, this);
                setupResponse(buffer, call, RpcStatusProto.FATAL, wrse.getErrorCode(),
                        null, ioe.getClass().getName(), ioe.getMessage());
                responder.doResponse(call);
                throw wrse;
            }
        }

        /**
         * 验证 rpc header 是否正确
         * @param header RPC request header
         * @throws WrappedRpcServerException header 包含无效值
         */
        private void checkRpcHeader(RpcRequestHeaderProto header)
                throws WrappedRpcServerException{
            if (!header.hasRpcKind()) {
                String errMsg = "IPC Server: No rpc kind in rpcRequestHeader";
                throw new WrappedRpcServerException(RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, errMsg);
            }
        }

        /**
         *  处理一个 RPC 请求，前提是 RPC header 和 context 已经读取
         * @param header
         * @param dis
         * @throws WrappedRpcServerException
         * @throws InterruptedException
         */
        private void processRpcRequest(RpcRequestHeaderProto header,
                                       DataInputStream dis)
                throws WrappedRpcServerException, InterruptedException {
            Class<? extends Writable> rpcRequestClass =
                    getRpcRequestWrapper(header.getRpcKind());
            if (rpcRequestClass == null) {
                LOG.warn("Unknown RPC kind " + header.getRpcKind() +
                        " from client " + hostAddress);
                final String err = "Unknown rpc kind in rpc header " + header.getRpcKind();
                throw new WrappedRpcServerException(
                        RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                        err);
            }
            Writable rpcRequest;
            try {
                Constructor<? extends Writable> constructor =
                        rpcRequestClass.getDeclaredConstructor();
                constructor.setAccessible(true);
                rpcRequest = constructor.newInstance();
                rpcRequest.readFields(dis);
            } catch (Exception e) {
                LOG.warn("Unable to read call parameters for client " +
                        hostAddress + "on connection protocol " +
                        this.protocolName + " for rpcKind " + header.getRpcKind(),  e);
                String err = "IPC server unable to read call parameters: "+ e.getMessage();
                throw new WrappedRpcServerException(
                        RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST, err);
            }
            Call call = new Call(header.getCallId(), header.getRetryCount(), rpcRequest, this,
                    ProtoUtil.converRpcKind(header.getRpcKind()), header.getClientId().toByteArray());
            // 将 call 入队列，有可能在这里阻塞
            callQueue.put(call);
            incRpcCount();
        }

        /**
         * 处理 out of band 数据
         * @param header RPC header
         * @param dis 请求的数据流
         * @throws WrappedRpcServerException 状态错误
         */
        private void processOutOfBandRequest(RpcRequestHeaderProto header,
                                             DataInputStream dis)
                throws WrappedRpcServerException {
            int callId = header.getCallId();
            if (callId == RpcConstants.CONNECTION_CONTEXT_CALL_ID) {
                processConnectionContext(dis);
            } else {
                throw new WrappedRpcServerException(
                        RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                        "Unknown out of band call #" + callId);
            }
        }

        /**
         * 读取 connection context 信息
         * @param dis 客户端请求的数据流
         * @throws WrappedRpcServerException 状态错误
         */
        private void processConnectionContext(DataInputStream dis)
                throws WrappedRpcServerException {
            if (connectionContextRead) {
                throw new WrappedRpcServerException(
                        RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                        "Connection context already processed");
            }
            connectionContext = decodeProtobufFromStream(
                    IpcConnectionContextProto.newBuilder(), dis);
            protocolName = connectionContext.getProtocol();
            connectionContextRead = true;
        }

        @SuppressWarnings("unchecked")
        private <T extends Message> T decodeProtobufFromStream(Message.Builder builder,
                                                               DataInputStream dis) throws WrappedRpcServerException {
            try {
                builder.mergeDelimitedFrom(dis);
                return (T) builder.build();
            } catch (Exception e) {
                Class<?> protoClass = builder.getDefaultInstanceForType().getClass();
                throw new WrappedRpcServerException(RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST,
                        "Error decoding " + protoClass.getSimpleName() + ": " + e);
            }
        }

        private boolean isIdle() {
            return rpcCount == 0;
        }

        private void incRpcCount() {
            rpcCount++;
        }

        private void decRpcCount() {
            rpcCount--;
        }

        private void setLastContact(long lastContact) {
            this.lastContact = lastContact;
        }

        private long getLastContact() {
            return lastContact;
        }

        // 可能多个线程都有可能调用该方法
        private synchronized void close() {
            data = null;
            dataLengthBuffer = null;
            if (!channel.isOpen()) {
                return;
            }
            try {
                socket.shutdownOutput();
            } catch (IOException e) {
                LOG.warn("Ignoring socket shutdown exception", e);
            }
            try {
                channel.close();
            } catch (IOException e) {
                LOG.warn("Ignoring channel close exception", e);
            }
            try {
                socket.close();
            } catch (IOException e) {
                LOG.warn("Ignoring socket close exception", e);
            }
        }
    }

    private class ConnectionManager {
        private final AtomicInteger count = new AtomicInteger();
        private Set<Connection> connections;

        final private Timer idleScanTimer;
        final private int idleScanThreshold;
        final private int idleScanInterval;
        final private int maxIdleTime;
        final private int maxIdleToClose;

        ConnectionManager() {
            this.idleScanTimer = new Timer(
                    "IPC Server idle connection scanner for port " + port, true);
            this.idleScanThreshold = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
            this.idleScanInterval = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT);
            this.maxIdleTime = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
            this.maxIdleToClose = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_DEFAULT);
            this.connections = Collections.newSetFromMap(
                    new ConcurrentHashMap<Connection, Boolean>(maxQueueSize));
        }

        private boolean add(Connection connection) {
            boolean added = connections.add(connection);
            if (added) {
                count.getAndIncrement();
            }
            return added;
        }

        private boolean remove(Connection connection) {
            boolean removed = connections.remove(connection);
            if (removed) {
                count.getAndDecrement();
            }
            return removed;
        }

        int size() {
            return count.get();
        }

        Connection register(SocketChannel channel) {
            Connection connection = new Connection(channel, System.currentTimeMillis());
            add(connection);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Server connection from " + connection +
                        "; # active connections: " + size() +
                        "; # queued calls: " + callQueue.size());
            }
            return connection;
        }

        /**
         * todo 如果关闭的时候，正好有线程在读，但还没有更新 lastContact 怎么办
         * @param connection
         * @return
         */
        private boolean close(Connection connection) {
            boolean exists = remove(connection);
            if (exists) {
                LOG.debug(Thread.currentThread().getName() +
                        ": disconnecting client " + connection +
                        ". Number of active connections: "+ size());
                connection.close();
            }
            return exists;
        }

        private void closeAll() {
            for (Connection connection : connections) {
                close(connection);
            }
        }

        /**
         * 当发生 OOM 时 listener 线程也会调用该方法，为了避免与定时器线程冲突
         * 需要加 synchronized
         * @param scanAll 是否扫描所有的 connection
         */
        synchronized void closeIdle(boolean scanAll) {
            long minLastContact = System.currentTimeMillis() - maxIdleTime;

            // 并发的 iterator 在迭代的过程中有可能遍历不到新插入的 Connection 对象
            // 但是没有关系，因为新的 Connection 不会是空闲的
            int closed = 0;
            for (Connection connection : connections) {
                // 不需要扫全部的情况下，如果没有达到扫描空闲 connection 的阈值
                // 或者下面代码关闭连接导致剩余连接小于 idleScanThreshold 时，便退出循环
                if (!scanAll && size() < idleScanThreshold) {
                    break;
                }

                // 关闭空闲的连接，由于 java && 的短路运算，
                // 如果 scanAll == true，只关闭 maxIdleToClose 个连接，否则全关闭
                if (connection.isIdle() &&
                    connection.getLastContact() < minLastContact &&
                    close(connection) &&
                    !scanAll && (++closed == maxIdleToClose)) {
                    break;
                }
            }
        }

        void startIdleScan() {
            scheduleIdleScanTask();
        }

        void stopIdleScan() {
            idleScanTimer.cancel();
        }

        private void scheduleIdleScanTask() {
            if (!running) {
                return;
            }
            TimerTask idleCloseTask = new TimerTask() {
                @Override
                public void run() {
                    if(!running) {
                        return;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName()+": task running");
                    }

                    try {
                        closeIdle(false);
                    } finally {
                        // 定时器只调度一次，所以本次任务执行完后手动再次添加到定时器中
                        scheduleIdleScanTask();
                    }
                }
            };
            idleScanTimer.schedule(idleCloseTask, idleScanInterval);
        }
    }
}
