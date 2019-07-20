package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author duma
 */
public abstract class Server {
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
    private CallQueueManager<Call> callQueue;

    private ConnectionManager connectionManager;
    private Listener listener;
    private Responder responder;

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
        //todo callQueue 初始化

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
     * 启动服务
     * todo synchronized 必须加的吗
     */
    public synchronized void start() {
        listener.start();
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

    public static class Call {

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

        Reader getReader() {
            currentReader = (currentReader + 1) % readers.length;
            return readers[currentReader];
        }
    }

    /**
     * 向客户端发送响应结果
     */
    private class Responder extends Thread {

    }

    private class Connection {
        private SocketChannel channel;
        private volatile long lastContact;

        private ByteBuffer data;
        private ByteBuffer dataLengthBuffer;
        private Socket socket;

        private volatile int rpcCount;

        Connection(SocketChannel channel, long lastContact) {
            this.channel = channel;
            this.lastContact = lastContact;
            this.socket = channel.socket();
        }

        private boolean isIdle() {
            return rpcCount == 0;
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
