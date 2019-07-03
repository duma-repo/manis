package com.cnblogs.duma.ipc;

import java.nio.ByteBuffer;

public class RpcConstants {
    /** RPC 连接发送 header 的头四个字节 */
    public static final ByteBuffer HEADER = ByteBuffer.wrap("mrpc".getBytes());

    public static final byte CURRENT_VERSION = 9;
}
