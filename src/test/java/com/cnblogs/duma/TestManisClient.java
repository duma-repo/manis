package com.cnblogs.duma;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.protocol.ClientProtocol;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestManisClient {

    @Test
    public void testManisClient() throws IOException {
        ManisClient manisClient = new ManisClient(URI.create("manis://localhost:9000"), new Configuration());

        assert manisClient.manisDb instanceof ClientProtocol;

        manisClient.getTableCount("db1", "tb1");
    }
}
