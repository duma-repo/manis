package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * @author duma
 */
public class RPC {
    final static int RPC_SERVICE_CLASS_DEFAULT = 0;
    public enum RpcKind {
        /**
         * RPC_SERIALIZABLE: SerializableRpcEngine
         * RPC_PROTOCOL_BUFFER: ProtoBufRpcEngine
         */
        RPC_SERIALIZABLE ((short) 1),
        RPC_PROTOCOL_BUFFER ((short) 2);

        public final short value;


        RpcKind(short value) {
            this.value = value;
        }
    }

    /**
     * 接口与RPC引擎对应关系的缓存
     */
    private static final Map<Class<?>, RpcEngine> PROTOCOL_ENGINS
            = new HashMap<Class<?>, RpcEngine>();

    public static final String RPC_ENGINE = "rpc.engine";

    /**
     * 为协议（接口）设置RPC引擎
     * @param conf 配置
     * @param protocol 协议接口
     * @param engine 实现的引擎
     */
    public static void setProtocolEngine(Configuration conf,
                                         Class<?> protocol, Class<?> engine) {
        conf.setClass(RPC_ENGINE + "." + protocol.getName(), engine, RpcEngine.class);
    }

    /**
     * 根据协议和配置返回该协议对应的RPC引擎
     * @param protocol
     * @param conf
     * @return
     */
    static synchronized <T> RpcEngine getProtocolEngine(Class<T> protocol, Configuration conf) {
        RpcEngine engine = PROTOCOL_ENGINS.get(protocol);
        if (engine == null) {
            Class<?> clazz = conf.getClass(RPC_ENGINE + "." + protocol.getName(), SerializableRpcEngine.class);

            try {
                // 通过反射实例化RpcEngine的实现类
                Constructor constructor =  clazz.getDeclaredConstructor();
                engine = (RpcEngine)constructor.newInstance();
                PROTOCOL_ENGINS.put(protocol, engine);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return engine;
    }

    /**
     * 获得协议版本
     * @param protocol
     * @return
     */
    public static long getProtocolVersion(Class<?> protocol) {
        if (protocol == null) {
            throw new IllegalArgumentException("Null protocol");
        }

        long version;
        ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
        if (anno != null) {
            version = anno.protocolVersion();
            if (version != -1) {
                return version;
            }
        }
        try {
            Field versionField = protocol.getField("versionID");
            versionField.setAccessible(true);
            return versionField.getLong(protocol);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getProtocolName(Class<?> protocol) {
        if (protocol == null) {
            return null;
        }

        ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
        return anno == null ? protocol.getName() : anno.protocolName();
    }

    public static <T> T getProtocolProxy(Class<T> protocol,
                                         long clientVersion,
                                         InetSocketAddress address,
                                         Configuration conf,
                                         SocketFactory factory,
                                         int rpcTimeOut)
        throws IOException {

        /**
         * todo 解析为什么需要new对象，而不是将getProxy定义成静态方法
         * 静态方法和单例对象的区别
         */
        return getProtocolEngine(protocol, conf).getProxy(protocol, clientVersion,
                address, conf, factory, rpcTimeOut);
    }

    /**
     * 该类用于构造 RPC Server
     */
    public static class Builder {
        private Class<?> protocol;
        private Object instance;
        private String bindAdress = "0.0.0.0";
        private int bindPort = 0;
        private int numHandlers = 1;
        private int numReaders = -1;
        private boolean verbose = false;
        private int queueSizePerHandler = -1;
        private Configuration conf;

        public Builder(Configuration conf) {
            this.conf = conf;
        }

        public Builder setProtocol(Class<?> protocol) {
            this.protocol = protocol;
            return this;
        }

        public Builder setInstance(Object instance) {
            this.instance = instance;
            return this;
        }

        public Builder setBindAdress(String bindAdress) {
            this.bindAdress = bindAdress;
            return this;
        }

        public Builder setBindPort(int bindPort) {
            this.bindPort = bindPort;
            return this;
        }

        public Builder setNumHandlers(int numHandlers) {
            this.numHandlers = numHandlers;
            return this;
        }

        public Builder setVerbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        public void setConf(Configuration conf) {
            this.conf = conf;
        }

        /**
         * 创建 RPC.Server 实例
         * @return RPC.Server
         * @throws IOException 发生错误
         * @throws IllegalArgumentException 没有设置必要的参数时
         */
        public Server build() throws IOException, IllegalArgumentException {
            if (this.conf == null) {
                throw new IllegalArgumentException("conf is not set");
            }
            if (this.protocol == null) {
                throw new IllegalArgumentException("protocol is not set");
            }
            if (this.instance == null) {
                throw new IllegalArgumentException("instance is not set");
            }

            return getProtocolEngine(this.protocol, this.conf).getServer(
                    this.protocol, this.instance, this.bindAdress, this.bindPort,
                    this.numHandlers, this.numReaders, this.queueSizePerHandler,
                    this.verbose, this.conf);
        }
    }

    /**
     * RPC Server
     */
    public abstract static class Server extends com.cnblogs.duma.ipc.Server {

    }
}
