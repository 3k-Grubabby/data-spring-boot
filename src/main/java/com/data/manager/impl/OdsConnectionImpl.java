package com.data.manager.impl;

import com.data.manager.Connection;
import org.springframework.stereotype.Component;

@Component
public class OdsConnectionImpl implements Connection<String> {
    @Override
    public void connect(String url) {
        // Ods特定的连接代码
        System.out.println("OdsConnection.connect");
    }

    @Override
    public void close() {
        // Ods特定的关闭代码
        System.out.println("OdsConnection.close");
    }

    @Override
    public String getConnection() {
        return null;
    }
}
