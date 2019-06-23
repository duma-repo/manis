package com.cnblogs.duma.client;

import com.cnblogs.duma.AppTest;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.ipc.Client;
import org.junit.Test;

import java.net.InetSocketAddress;

public class TestConnectionId {
    @Test
    public void testNewConnectionId() {
        Client.ConnectionId remoteId = new Client.ConnectionId(new InetSocketAddress(9000),
                AppTest.class, 1000, new Configuration());
        assert remoteId != null;

        assert remoteId.getProtocol().equals(AppTest.class);
        assert remoteId.getRpcTimeOut() == 1000;
        assert remoteId.isDoPing() == true;
        assert remoteId.isTcpNoDelay() == true;
        assert remoteId.getMaxIdleTime() == 10000;
        assert remoteId.getPingInterval() == 60000;
    }

    @Test
    public void testEquals() {
        Client.ConnectionId remoteId1 = new Client.ConnectionId(new InetSocketAddress(9000),
                AppTest.class, 1000, new Configuration());
        System.out.println(remoteId1.hashCode());

        Client.ConnectionId remoteId2 = new Client.ConnectionId(new InetSocketAddress(8000),
                AppTest.class, 1000, new Configuration());
        System.out.println(remoteId2.hashCode());

        Client.ConnectionId remoteId3 = new Client.ConnectionId(new InetSocketAddress(9000),
                AppTest.class, 1000, new Configuration());
        System.out.println(remoteId3.hashCode());

        assert !remoteId1.equals(remoteId2);
        assert remoteId1.equals(remoteId3);
        assert remoteId1.equals(remoteId1);
    }
}
