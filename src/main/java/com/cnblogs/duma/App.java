package com.cnblogs.duma;

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
        System.out.println(System.getProperty("user.dir"));
        String confFilePath = App.class.getClassLoader().getResource("manis.properties").getPath();
        Properties properties = new Properties();
        properties.load(new FileInputStream(confFilePath));
        System.out.println(properties.getProperty("manis.test"));
        System.out.println(URI.create("manis://asdsad:::sdfsdf").getAuthority());
    }
}
