package com.cnblogs.duma.client;

import com.cnblogs.duma.AppTest;
import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.ipc.Client;
import com.cnblogs.duma.ipc.Client.ConnectionId;
import com.cnblogs.duma.ipc.protobuf.ProtobufRpcEngineProtos;
import com.google.protobuf.Message;
import org.junit.Test;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class TestConnection {

    public static void main(String[] args) throws IOException {
        boolean isTest = true;
        if (!isTest) return;
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.socket().bind(new InetSocketAddress("127.0.0.1", 8000));
        ssc.configureBlocking(false);
        Selector selector = Selector.open();

        // 注册 channel，并且指定感兴趣的事件是 Accept
        ssc.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("select wait");
        int nReady = selector.select();
        Set<SelectionKey> keys = selector.selectedKeys();
        System.out.println("select start");
        Iterator<SelectionKey> it = keys.iterator();

        if (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();

            if (key.isAcceptable()) {
                // 创建新的连接，并且把连接注册到selector上，而且，
                // 声明这个channel只对读操作感兴趣。
                System.out.println("get connect");
                SocketChannel socketChannel = ssc.accept();
                socketChannel.configureBlocking(false);
                socketChannel.socket().setKeepAlive(true);
                socketChannel.register(selector, SelectionKey.OP_WRITE);
            }
        }
    }

    @Test
    public void testSetupConnection() throws Exception {
        Configuration conf = new Configuration();
        // 1ms 模拟连接超时
        conf.set(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY, "1");
        int a = conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);
        System.out.println(a);

        Client.ConnectionId remoteId = new Client.ConnectionId(new InetSocketAddress("127.0.0.1", 8000),
                AppTest.class, 1000, conf);

        Client client = new Client(null, new Configuration(), SocketFactory.getDefault());

        /**
         * 构造 Connection 对象
         * private 内部类的构造方法的参数需要传入外部类
         */
        Class clazz = Class.forName("com.cnblogs.duma.ipc.Client$Connection");
        Constructor classConnectionConstructor = clazz.getConstructor(Client.class, ConnectionId.class, Integer.class);
        classConnectionConstructor.setAccessible(true);
        Object connection = classConnectionConstructor.newInstance(client, remoteId, 0);

        /**
         * 调用 setupConnection 方法
         */
        Method methodSetupConnection = clazz.getDeclaredMethod("setupConnection");
        methodSetupConnection.setAccessible(true);
        methodSetupConnection.invoke(connection);
    }

}
