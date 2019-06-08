package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.ObjectWritable;
import com.cnblogs.duma.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

/**
 * @author duma
 */
public class SerializableRpcEngine implements RpcEngine {
    public static final Log LOG = LogFactory.getLog(SerializableRpcEngine.class);

    private static class Invocation implements Writable {
        public Invocation(Method method, Object[] args) {

        }

        @Override
        public void write(DataOutput out) throws IOException {

        }

        @Override
        public void readFields(DataInput in) throws IOException {

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
