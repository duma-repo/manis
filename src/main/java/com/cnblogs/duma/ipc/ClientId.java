package com.cnblogs.duma.ipc;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * 该类定义了一些静态方法，提供了client id 在字节数组和字符串之间的转换
 * client id 基于 UUID
 * @author duma
 */
public class ClientId {
    /** UUID 的字节数组长度：16 */
    public static final int BYTE_LENGTH = 16;

    public static byte[] getClientId() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[BYTE_LENGTH]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        return byteBuffer.array();
    }
}
