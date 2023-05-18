package com.data.service;

import java.util.List;

public interface DataService<T> {
    void execute(String params);
    T getConnection(String url);
    void destroy();
}