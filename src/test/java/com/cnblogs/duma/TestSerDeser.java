package com.cnblogs.duma;

import com.cnblogs.duma.ipc.SerializableRpcEngine;
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
        Constructor constructor = clazz.getConstructor(new Class[]{Method.class, Object[].class});
        constructor.setAccessible(true);
        Method method = TestSerDeser.class.getMethod("testFunc", new Class[]{int.class, int.class});
        Object[] args = new Object[]{1, 2};
        Object object = constructor.newInstance(method, args);

        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bo);
        Method methodWrite = clazz.getMethod("write", new Class[]{DataOutput.class});
        methodWrite.setAccessible(true);
        methodWrite.invoke(object, out);
        System.out.println(out.size());

        ByteArrayInputStream baInputStream = new ByteArrayInputStream(bo.toByteArray());
        DataInput in = new DataInputStream(baInputStream);

        Constructor defaultConstructor = clazz.getDeclaredConstructor(new Class[]{});
        defaultConstructor.setAccessible(true);
        Object obj2 = defaultConstructor.newInstance();
        System.out.println(obj2);

        Method methodRead = clazz.getMethod("readFields", new Class[]{DataInput.class});
        methodRead.setAccessible(true);
        methodRead.invoke(obj2, in);

        System.out.println(obj2);
    }

    public void testFunc(int a, int b) {

    }

    public void testFunc2(int a) {

    }
}
