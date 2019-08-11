package com.cnblogs.duma;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.protocol.ClientProtocol;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException {
//        System.out.println(System.getProperty("user.dir"));
//        String confFilePath = App.class.getClassLoader().getResource("manis.properties").getPath();
//        Properties properties = new Properties();
//        properties.load(new FileInputStream(confFilePath));
//        System.out.println(properties.getProperty("manis.test"));
//        System.out.println(URI.create("manis://asdsad:::sdfsdf").getAuthority());

        ManisClient manisClient = new ManisClient(URI.create("manis://localhost:8866"), new Configuration());
        int res = manisClient.getTableCount("db1", "tb1");
        System.out.println(res);
    }
}
