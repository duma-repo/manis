package com.cnblogs.duma.conf;

import java.util.Properties;

/**
 * @author duma
 */
public class Configuration {
    Properties properties;

    public Configuration() {
        //todo init properties
        properties = new Properties();
    }

    public void set(String name, String value) {
        properties.setProperty(name, value);
    }

    public String get(String name) {
        return properties.getProperty(name);
    }

    public String get(String name, String defaultValue) {
        return properties.getProperty(name, defaultValue);
    }

    public void setClass(String name, Class<?> theClass, Class<?> xface) {
        if (!xface.isAssignableFrom(theClass)) {
            throw new RuntimeException(theClass + " not " + xface.getName());
        }

        set(name, theClass.getName());
    }

    public Class<?> getClassByName(String clsName) throws ClassNotFoundException {
        return Class.forName(clsName);
    }

    public Class<?> getClass(String name, Class<?> defaultValue) {
        String className = get(name);
        if (className == null) {
            return defaultValue;
        }

        try {
            return getClassByName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
