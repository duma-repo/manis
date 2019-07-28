package com.cnblogs.duma.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class StringUtils {

    public static String stringifyException(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter printWriter = new PrintWriter(sw);
        e.printStackTrace(printWriter);
        printWriter.close();
        return sw.toString();
    }
}
