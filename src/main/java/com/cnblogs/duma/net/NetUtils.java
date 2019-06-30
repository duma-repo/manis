package com.cnblogs.duma.net;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.*;
import java.nio.channels.Channel;

public class NetUtils {
    private static final Log LOG = LogFactory.getLog(NetUtils.class);

    /**
     * {@link Socket#connect(SocketAddress, int)} 的替代函数
     * 对于没有关联 channel 的一般 socket，调用 <code>socket.connect(endpoint, timeout)</code> 进行连接
     * 如果 <code>socket.getChannel()</code> 返回非空的 channel, 便使用 Hadoop selector 的方式进行连接
     * Hadoop 自定义 selector 主要是为了获得连接的控制权
     *
     * @param socket socket
     * @param endPint 远程地址
     * @param timeout 连接超时时间，单位：ms
     * @throws IOException
     */
    public static void connect(Socket socket,
                               InetSocketAddress endPint, int timeout) throws IOException {
        if (socket == null || endPint == null || timeout < 0) {
            throw new IllegalArgumentException("Illegal Argument for connect()");
        }

        Channel ch = socket.getChannel();
        if (ch == null) {
            socket.connect(endPint, timeout);
        } else {
            /**
             * todo 使用自定义的 socket 连接
             */
        }

        /**
         * TCP 会出现一种罕见的情况 —— 自连接。
         * 在服务端 down 掉的情况下，客户端连接本地的服务端有可能连接成功
         * 且客户端的地址和端口与服务端相同。这会导致本地再次启动服务端失败，因为端口和地址被占用
         * 因此，出现这种情况视为连接拒绝
         */
        if (socket.getLocalAddress().equals(socket.getInetAddress()) &&
            socket.getLocalPort() == socket.getPort()) {
            LOG.info("Detected a loopback TCP socket, disconnecting it");
            socket.close();
            throw new ConnectException("Localhost targeted connection resulted in a loopback. " +
                    "No daemon is listening on the target port."
            );
        }
    }
}
