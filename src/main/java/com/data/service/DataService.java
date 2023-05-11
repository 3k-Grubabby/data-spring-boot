package com.data.service;

import java.util.List;

public interface DataService {
    void execute(String params);


    void conn(String url);

    void close();
}