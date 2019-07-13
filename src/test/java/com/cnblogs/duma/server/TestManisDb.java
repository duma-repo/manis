package com.cnblogs.duma.server;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.server.manisdb.ManisDb;
import org.junit.Test;

import java.io.IOException;

public class TestManisDb {

    @Test
    public void testMain() throws IOException {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.MANIS_RPC_PROTOBUF_KEY, "manis://localhost:8866");
        ManisDb manisDb = new ManisDb(conf);
        assert manisDb != null;
    }
}
