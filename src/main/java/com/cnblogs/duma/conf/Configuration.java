package com.cnblogs.duma.conf;

import java.util.Properties;

/**
 * @author duma
 */
public class Configuration {
    Properties properties;

    public Configuration() {

    }

    public String get(String name) {
        return properties.getProperty(name);
    }

    public String get(String name, String defaultValue) {
        return properties.getProperty(name, defaultValue);
    }
}
