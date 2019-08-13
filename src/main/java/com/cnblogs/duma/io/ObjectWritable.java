package com.cnblogs.duma.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ObjectWritable implements Writable {

    private Class declaredClass;
    private Object instance;

    public ObjectWritable() {}

    public ObjectWritable(Class declaredClass, Object instance) {
        this.declaredClass = declaredClass;
        this.instance = instance;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
