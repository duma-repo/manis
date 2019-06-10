package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.ObjectWritable;
import com.cnblogs.duma.io.Writable;
import com.sun.xml.internal.ws.api.ha.StickyFeature;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.*;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

/**
 * @author duma
 */
public class SerializableRpcEngine implements RpcEngine {
    public static final Log LOG = LogFactory.getLog(SerializableRpcEngine.class);

    private static class Invocation implements Writable {
        private String methodName;
        private Class<?>[] parameterClasses;
        private Object[] parameters;
        private String declaringClassProtocolName;

        public Invocation(Method method, Object[] args) {
            this.methodName = method.getName();
            this.parameterClasses = method.getParameterTypes();
            this.parameters = args;
            this.declaringClassProtocolName =
                    RPC.getProtocolName(method.getDeclaringClass());
        }

        @Override
        public void write(DataOutput out) throws IOException {
            ByteArrayOutputStream byteArrOut = new ByteArrayOutputStream();
            ObjectOutputStream objOut = new ObjectOutputStream(byteArrOut);

            objOut.writeObject(declaringClassProtocolName);
            objOut.writeObject(methodName);
            objOut.writeObject(parameters);

            objOut.flush();

            out.write(byteArrOut.toByteArray().length);
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
                declaringClassProtocolName = (String) objIn.readObject();
                methodName = (String) objIn.readObject();
                parameters = (Object []) objIn.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new IOException("Class not found when deserialize.");
            }
        }
    }

    private static class Invoker implements RpcInvocationHandler {
        private Client.ConnectionId remoteId;
        private Client client;

        private Invoker(Class<?> protocol, InetSocketAddress address,
                Configuration conf, SocketFactory factory,
                int rpcTimeOut)
                throws IOException {
            //todo init client
            System.out.println("init Invoker in SerializableRpcEngine.");
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            long startTime = 0;
            if (LOG.isDebugEnabled()) {
                startTime = System.currentTimeMillis();
            }
            ObjectWritable value;
            value = (ObjectWritable) client.call(RPC.RpcKind.RPC_SERIALIZABLE,
                    new Invocation(method, args), this.remoteId);
            if (LOG.isDebugEnabled()) {
                long callTime = System.currentTimeMillis() - startTime;
                LOG.debug("Call " + method.getName() + " " + callTime);
            }
            return value;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProxy(Class<T> protocol, long clientVersion,
                          InetSocketAddress address, Configuration conf,
                          SocketFactory factory, int rpcTimeOut)
            throws IOException {

        final Invoker invoker = new Invoker(protocol, address, conf, factory, rpcTimeOut);
        return (T) Proxy.newProxyInstance(protocol.getClassLoader(), new Class[]{protocol}, invoker);
    }
}
