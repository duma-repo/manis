package com.cnblogs.duma.protocol;

import com.cnblogs.duma.ipc.ProtocolInfo;

/**
 * @author duma
 */
@ProtocolInfo(protocolName = ManisConstants.MANAGER_MANISDB_PROTOCOL_NAME,
        protocolVersion = 1)
public interface ManagerManisDbPritocolSerializable
        extends ManagerProtocol {
}
