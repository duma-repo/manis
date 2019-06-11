package com.cnblogs.duma;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.protocol.ManagerProtocol;
import com.cnblogs.duma.protocolPB.ClientManisDbProtocolTranslatorPB;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestManager {

    @Test
    public void testManager() throws IOException {
        Manager manager = new Manager(URI.create("manis://localhost:9000"), new Configuration());

        assert manager.manisDb instanceof ManagerProtocol;
    }
}
