package com.cnblogs.duma.ipc;

import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.*;

import java.io.IOException;

public class RpcServerException extends IOException {
    public RpcServerException(String message) {
        super(message);
    }

    public RpcServerException(String message, Throwable cause) {
        super(message, cause);
    }

    public RpcStatusProto getRpcStatusProto() {
        return RpcStatusProto.ERROR;
    }

    public RpcErrorCodeProto getRpcErrorCode() {
        return RpcErrorCodeProto.ERROR_RPC_SERVER;
    }
}
