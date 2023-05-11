package com.data.controller;

import com.data.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IndexController {
    @Value("${data.esp.url}")
    private String espUrl;

    @Autowired
    private DataService dataService;
    @GetMapping("/")
    public void index() {
        dataService.execute("{'startTime': '2020-01-01 00:00:00','offset':'299000','interval':'300','url':'"+espUrl+"'}");
    }

    @GetMapping("/test")
    public void test() {
        dataService.execute("{'startTime': '2020-01-01 00:00:00', 'endTime': '2020-01-01 00:00:00','offset':'299000','interval':'300','url':'"+espUrl+"'}");
    }

}
