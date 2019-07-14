package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.List;

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
//    private CallQueueManager<Call> callQueue;

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
        // todo 连接相关的对象
        this.tcpNoDelay = conf.getBoolean(
                CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT);

        // 创建 responder
        this.responder = new Responder();
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
     * socket 监听线程
     */
    private class Listener extends Thread {
        private ServerSocketChannel acceptChannel;
        private Selector selector;
        private Reader[] readers;
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
            Reader(String name) {
                super(name);
            }
        }
    }

    /**
     * 向客户端发送响应结果
     */
    private class Responder extends Thread {

    }
}
