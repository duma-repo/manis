package com.cnblogs.duma.protocolPB;

import com.cnblogs.duma.ipc.ProtocolInfo;
import com.cnblogs.duma.protocol.ManisConstants;
import com.cnblogs.duma.protocol.proto.ClientManisDbProtocolProtos.ClientManisDbProtocol;

/**
 * @author duma
 * todo 实现protobuf定义的接口
 */
@ProtocolInfo(protocolName = ManisConstants.CLIENT_MANISDB_PROTOCOL_NAME,
        protocolVersion = 1)
public interface ClientManisDbProtocolPB extends
        ClientManisDbProtocol.BlockingInterface {
}
