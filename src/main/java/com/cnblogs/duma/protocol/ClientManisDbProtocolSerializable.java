package com.cnblogs.duma.protocol;

import com.cnblogs.duma.ipc.ProtocolInfo;

/**
 * @author duma
 */
@ProtocolInfo(protocolName = ManisConstants.CLIENT_NAMENODE_PROTOCOL_NAME,
        protocolVersion = 1)
public interface ClientManisDbProtocolSerializable extends
    ClientProtocol {
}
