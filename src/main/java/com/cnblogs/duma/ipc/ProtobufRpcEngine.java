package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.DataOutputOutputStream;
import com.cnblogs.duma.io.Writable;
import com.cnblogs.duma.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import com.cnblogs.duma.util.ProtoUtil;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.*;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

public class ProtobufRpcEngine implements RpcEngine {
    public static  final Log LOG = LogFactory.getLog(ProtobufRpcEngine.class);

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProxy(Class<T> protocol,
                          long clientVersion,
                          InetSocketAddress address,
                          Configuration conf,
                          SocketFactory factory,
                          int rpcTimeOut) throws IOException {
        Invoker invoker = new Invoker(protocol, address, conf, factory, rpcTimeOut);
        return (T) Proxy.newProxyInstance(protocol.getClassLoader(), new Class[]{protocol}, invoker);
    }

    private static class Invoker implements RpcInvocationHandler {
        private Client client;
        private Client.ConnectionId remoteId;
        private final String protocolName;
        private final int NORMAL_ARGS_LEN = 2;

        private Invoker(Class<?> protocol,
                       InetSocketAddress address,
                       Configuration conf,
                       SocketFactory factory,
                       int rpcTimeOut) {
            this.protocolName = RPC.getProtocolName(protocol);
            this.remoteId = new Client.ConnectionId(address, protocol, rpcTimeOut, conf);
            this.client = new Client(null, conf, factory);
            System.out.println("init Invoker in ProtobufRpcEngine.");
        }

        private RequestHeaderProto constructRpcRequesHeader(Method method) {
            RequestHeaderProto.Builder headerBuilder = RequestHeaderProto
                    .newBuilder();
            headerBuilder.setMethodName(method.getName());
            headerBuilder.setDeclaringClassProtocolName(protocolName);

            return headerBuilder.build();
        }

        /**
         * RPC 在客户端的 invoker
         * 上层希望仅有 ServiceException 异常被抛出，因此该方法仅抛出 ServiceException 异常
         *
         * 以下两种情况都构造 ServiceException :
         * <ol>
         * <li>该方法中客户端抛出的异常</li>
         * <li>服务端的异常包装在 RemoteException 中的异常</li>
         * </ol>
         *
         * @param proxy proxy
         * @param method 调用的方法
         * @param args 参数
         * @return 返回值
         * @throws ServiceException 异常
         */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws ServiceException {
            long startTime = 0;
            if (LOG.isDebugEnabled()) {
                startTime = System.currentTimeMillis();
            }

            if (args.length != NORMAL_ARGS_LEN) {
                throw new ServiceException("Too many parameters for request. Method: ["
                        + method.getName() + "]" + ", Expected: 2, Actual: "
                        + args.length);
            }
            if (args[1] == null) {
                throw new ServiceException("null param while calling Method: ["
                        + method.getName() + "]");
            }

            RequestHeaderProto header = constructRpcRequesHeader(method);
            Message theRequest = (Message) args[1];
            final Object res;
            try {
                res = client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                        new RpcRequestWrapper(header, theRequest), remoteId);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    interface RpcWrapper extends Writable {
        int getLength();
    }

    /**
     * todo 装饰者模式
     * @param <T>
     */
    private static abstract class BaseRpcMessageWithHeader<T extends GeneratedMessage>
            implements RpcWrapper {
        T requestHeader;
        /**
         * 用于客户端
         */
        Message theRequest;
        /**
         * 用于服务端
         */
        byte[] theRequestRead;

        public BaseRpcMessageWithHeader() {}

        public BaseRpcMessageWithHeader(T requestHeader, Message theRequest) {
            this.requestHeader = requestHeader;
            this.theRequest = theRequest;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            OutputStream os = DataOutputOutputStream.constructDataOutputStream(out);

            requestHeader.writeDelimitedTo(os);
            theRequest.writeDelimitedTo(os);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            requestHeader = parseHeaderFrom(readVarIntBytes(in));
            theRequestRead = readMessageRequest(in);
        }

        /**
         * 子类会覆盖该方法
         * @param in
         * @return
         */
        byte[] readMessageRequest(DataInput in) throws IOException {
            return readVarIntBytes(in);
        }

        private byte[] readVarIntBytes(DataInput in) throws IOException {
            int length = ProtoUtil.readRawVarInt32(in);
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return bytes;
        }

        /**
         * todo 这是一个设计模式
         * @param bytes
         * @return
         * @throws IOException
         */
        abstract T parseHeaderFrom(byte[] bytes) throws IOException;

        /**
         * 包括两部分
         * 1. header 序列化后的长度及长度本身的 varInt32 编码后的长度
         * 2. request 序列化后的长度以及长度本身 varInt32 编码后的长度
         * @return
         */
        @Override
        public int getLength() {
            int headerLen = requestHeader.getSerializedSize();
            int requestLen;
            if (theRequest != null) {
                requestLen = theRequest.getSerializedSize();
            } else if (theRequestRead != null) {
                requestLen = theRequestRead.length;
            } else {
                throw new IllegalArgumentException("getLength on uninitialized RpcWrapper");
            }
            return CodedOutputStream.computeRawVarint32Size(headerLen) + headerLen +
                    CodedOutputStream.computeRawVarint32Size(requestLen) + requestLen;
        }
    }

    private static class RpcRequestWrapper extends
            BaseRpcMessageWithHeader<RequestHeaderProto> {
        @SuppressWarnings("unused")
        public RpcRequestWrapper() {}

        public RpcRequestWrapper(RequestHeaderProto requestHeader, Message theRequest) {
            super(requestHeader, theRequest);
        }

        @Override
        RequestHeaderProto parseHeaderFrom(byte[] bytes) throws IOException {
            return RequestHeaderProto.parseFrom(bytes);
        }

        @Override
        public String toString() {
            return requestHeader.getDeclaringClassProtocolName() + "." +
                    requestHeader.getMethodName();
        }
    }

    public static class RpcRequestMessageWrapper extends
            BaseRpcMessageWithHeader<RpcRequestHeaderProto> {

        public RpcRequestMessageWrapper(RpcRequestHeaderProto requestHeader, Message theRequest) {
            super(requestHeader, theRequest);
        }

        @Override
        RpcRequestHeaderProto parseHeaderFrom(byte[] bytes) throws IOException {
            return RpcRequestHeaderProto.parseFrom(bytes);
        }
    }

    @Override
    public RPC.Server getServer(Class<?> protocol, Object instance,
                                String bindAddress, int port,
                                int numHandlers, int numReaders,
                                int queueSizePerHandler, boolean verbose,
                                Configuration conf) throws IOException {
        return null;
    }
}
