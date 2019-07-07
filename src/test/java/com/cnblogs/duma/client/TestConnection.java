package com.cnblogs.duma.client;

import com.cnblogs.duma.AppTest;
import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.Writable;
import com.cnblogs.duma.ipc.Client;
import com.cnblogs.duma.ipc.Client.ConnectionId;
import com.cnblogs.duma.ipc.ProtobufRpcEngine.*;
import com.cnblogs.duma.ipc.RPC;
import com.cnblogs.duma.ipc.RpcConstants;
import com.cnblogs.duma.ipc.SerializableRpcEngine;
import com.cnblogs.duma.ipc.protobuf.IpcConnectionContextProtos.*;
import com.cnblogs.duma.ipc.protobuf.ProtobufRpcEngineProtos;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.*;
import com.cnblogs.duma.util.ProtoUtil;
import com.google.protobuf.Message;
import org.junit.Test;

import javax.net.SocketFactory;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
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

//    @Test
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

    public Client getClient(Configuration conf) {
        Client.ConnectionId remoteId = new Client.ConnectionId(new InetSocketAddress("127.0.0.1", 8000),
                AppTest.class, 1000, conf);

        return new Client(null, new Configuration(), SocketFactory.getDefault());
    }

    public ConnectionId getRemoteId(Configuration conf) {
        return new Client.ConnectionId(new InetSocketAddress("127.0.0.1", 8000),
                AppTest.class, 1000, conf);
    }

    public Object getConnection(Client client, ConnectionId remoteId, Configuration conf) throws Exception {
        /**
         * 构造 Connection 对象
         * private 内部类的构造方法的参数需要传入外部类
         */
        Class clazz = Class.forName("com.cnblogs.duma.ipc.Client$Connection");
        Constructor classConnectionConstructor = clazz.getConstructor(Client.class, ConnectionId.class, Integer.class);
        classConnectionConstructor.setAccessible(true);
        Object connection = classConnectionConstructor.newInstance(client, remoteId, 0);
        return connection;
    }

    public Method getMethod(Class clazz, String methodName, Class<?>... parameterTypes)
            throws NoSuchMethodException {
        /**
         * 调用 setupConnection 方法
         */
        Method method = clazz.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method;
    }

    public Field getField(Class clazz, String fieldName) throws NoSuchFieldException {
        Field field = null;
        try {
            field = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            field = clazz.getSuperclass().getDeclaredField(fieldName);
        }
        field.setAccessible(true);
        return field;
    }

    public OutputStream getOutPutStream(OutputStream bo) {
        return new DataOutputStream(bo);
    }

    @Test
    public void testWriteConnectionContext() throws Exception {
        Configuration conf = new Configuration();
        Client client = getClient(conf);
        ConnectionId remoteId = getRemoteId(conf);

        Object connection = getConnection(client, remoteId, conf);
        Class clazz = connection.getClass();
        Method methodWriteConnectionContext =
                getMethod(
                        clazz,
                        "writeConnectionContext",
                        new Class[]{ConnectionId.class});


        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        OutputStream outStream = getOutPutStream(bo);

        Field fieldOut = getField(clazz, "out");
        fieldOut.set(connection, outStream);

        methodWriteConnectionContext.invoke(connection, remoteId);

        assert bo.size() != 0;

        /**
         * 反序列化
         */
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(bo.toByteArray());
        DataInput in = new DataInputStream(baInputStream);
        assert in.readInt() != 0;
        RpcRequestMessageWrapper request = new RpcRequestMessageWrapper(null, null);
        request.readFields(in);

        Field requestHeaderField = getField(RpcRequestMessageWrapper.class, "requestHeader");
        RpcRequestHeaderProto requestHeader = (RpcRequestHeaderProto)requestHeaderField.get(request);
        assert requestHeader != null;
        assert requestHeader.getRetryCount() == RpcConstants.INVALID_RETRY_COUNT;

        Field theRequestReadField = getField(RpcRequestMessageWrapper.class, "theRequestRead");
        byte[] theRequestRead = (byte[])theRequestReadField.get(request);
        IpcConnectionContextProto connectionContext = IpcConnectionContextProto.parseFrom(theRequestRead);
        assert connectionContext != null;
        assert connectionContext.getProtocol().equals(RPC.getProtocolName(AppTest.class));
    }

    @Test
    public void testSendRpcRequest() throws Exception {
        Configuration conf = new Configuration();
        Client client = getClient(conf);
        ConnectionId remoteId = getRemoteId(conf);

        Object connection = getConnection(client, remoteId, conf);

        Class clazzClient = client.getClass();
        Class clazzConnection = connection.getClass();

        /**
         *
         */
        Class clazzCall = null;
        for (Class clazz : clazzClient.getDeclaredClasses()) {
            if (clazz.getName().equals("com.cnblogs.duma.ipc.Client$Call")) {
                clazzCall = clazz;
            }
        }
        Method sendRpcRequestMethod =
                getMethod(clazzConnection, "sendRpcRequest", new Class[]{clazzCall});

        Method createCallMethod =
                getMethod(clazzClient, "createCall", new Class[]{RPC.RpcKind.class, Writable.class});

        TestInvocation request = new TestInvocation();
        Object call = createCallMethod.invoke(client, RPC.RpcKind.RPC_SERIALIZABLE, request);

        /**
         * set client out field
         */
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        OutputStream outStream = getOutPutStream(bo);
        Field fieldOut = getField(clazzConnection, "out");
        fieldOut.set(connection, outStream);

        sendRpcRequestMethod.invoke(connection, call);

        ByteArrayInputStream baInputStream = new ByteArrayInputStream(bo.toByteArray());
        DataInput in = new DataInputStream(baInputStream);
        assert in.readInt() != 0;

        int headerLen = ProtoUtil.readRawVarInt32(in);
        byte[] headerBs = new byte[headerLen];
        in.readFully(headerBs);
        RpcRequestHeaderProto header = RpcRequestHeaderProto.parseFrom(headerBs);
        assert header != null;
        assert header.getRpcKind() == RpcKindProto.RPC_SERIALIZABLE;

        request.readFields(in);
    }

    public static class TestInvocation implements Writable {

        @Override
        public void write(DataOutput out) throws IOException {
            ByteArrayOutputStream byteArrOut = new ByteArrayOutputStream();
            ObjectOutputStream objOut = new ObjectOutputStream(byteArrOut);

            objOut.writeObject("test");
            objOut.flush();

            out.writeInt(byteArrOut.toByteArray().length);
            out.write(byteArrOut.toByteArray());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int length = in.readInt();
            byte[] byteArr = new byte[length];
            in.readFully(byteArr);

            ByteArrayInputStream byteArrIn = new ByteArrayInputStream(byteArr);
            ObjectInputStream objIn = new ObjectInputStream(byteArrIn);

            try {
                String f0 = (String) objIn.readObject();
                assert f0.equals("test");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new IOException("Class not found when deserialize.");
            }
        }
    }
}
