package com.data.manager;

public interface Connection<T> {
    void connect(String url);
    void close();

    T getConnection();

}
