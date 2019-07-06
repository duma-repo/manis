package com.cnblogs.duma;

import com.cnblogs.duma.io.DataOutputOutputStream;
import com.cnblogs.duma.ipc.SerializableRpcEngine;
import com.cnblogs.duma.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import com.cnblogs.duma.protocol.proto.ClientManisDbProtocolProtos.GetTableCountRequestProto;
import com.google.protobuf.Message;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class TestSerDeser {

    @Test
    public void testSerDeser() throws Exception {
        Class clazz = Class.forName("com.cnblogs.duma.ipc.SerializableRpcEngine$Invocation");
        Constructor constructor = clazz.getConstructor(Method.class, Object[].class);
        constructor.setAccessible(true);
        Method method = TestSerDeser.class.getMethod("testFunc", int.class, int.class);
        Object[] args = new Object[]{1, 2};
        Object object = constructor.newInstance(method, args);

        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bo);
        Method methodWrite = clazz.getMethod("write", DataOutput.class);
        methodWrite.setAccessible(true);
        methodWrite.invoke(object, out);
        System.out.println(out.size());

        ByteArrayInputStream baInputStream = new ByteArrayInputStream(bo.toByteArray());
        DataInput in = new DataInputStream(baInputStream);

        Constructor defaultConstructor = clazz.getDeclaredConstructor();
        defaultConstructor.setAccessible(true);
        Object obj2 = defaultConstructor.newInstance();

        Method methodRead = clazz.getMethod("readFields", DataInput.class);
        methodRead.setAccessible(true);
        methodRead.invoke(obj2, in);

        System.out.println(obj2);
    }

    public void testFunc(int a, int b) {

    }

    @Test
    public void testRpcRequestWrapper() throws Exception {
        RequestHeaderProto header = RequestHeaderProto.newBuilder()
                .setMethodName("testRpcRequestWrapper")
                .setDeclaringClassProtocolName("testProtocolName")
                .build();
        GetTableCountRequestProto req = GetTableCountRequestProto.newBuilder()
                .setDbName("db1")
                .setTbName("tb1")
                .build();

        Class clazz = Class.forName("com.cnblogs.duma.ipc.ProtobufRpcEngine$RpcRequestWrapper");
        Constructor constructor = clazz.getConstructor(RequestHeaderProto.class, Message.class);
        constructor.setAccessible(true);
        Object rpcReqWrapper = constructor.newInstance(header, req);
        System.out.println(rpcReqWrapper);

        /**
         * 测试序列化方法 write
         */
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bo);
        Method writeMethod = clazz.getMethod("write", DataOutput.class);
        writeMethod.setAccessible(true);
        writeMethod.invoke(rpcReqWrapper, out);
        System.out.println("write size: " + bo.toByteArray().length);

        /**
         * 测试反序列化方法 readFields
         */
        Constructor defaultConstructor = clazz.getConstructor();
        defaultConstructor.setAccessible(true);
        Object newRpcReqWrapper = defaultConstructor.newInstance();

        Field theRequestReadField = clazz.getSuperclass().getDeclaredField("theRequestRead");
        theRequestReadField.setAccessible(true);
        byte[] theRequestRead = (byte[]) theRequestReadField.get(newRpcReqWrapper);
        assert theRequestRead == null;

        ByteArrayInputStream baInputStream = new ByteArrayInputStream(bo.toByteArray());
        DataInput in = new DataInputStream(baInputStream);
        Method readFieldsMethod = clazz.getMethod("readFields", DataInput.class);
        readFieldsMethod.setAccessible(true);
        readFieldsMethod.invoke(newRpcReqWrapper, in);

        /**
         * 判断 requestHeader 字段反序列化
         */
        assert newRpcReqWrapper.toString().equals(rpcReqWrapper.toString());

        /**
         * 判断 theRequestRead 字段反序列化
         */
        theRequestRead = (byte[]) theRequestReadField.get(newRpcReqWrapper);
        GetTableCountRequestProto deSerReq = GetTableCountRequestProto.parseFrom(theRequestRead);
        assert deSerReq.getDbName().equals("db1");
        assert deSerReq.getTbName().equals("tb1");
    }
}
