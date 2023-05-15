package com.data.controller;

import com.data.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IndexController {

    @Autowired
    private DataService dataService;

    @GetMapping("/")
    public void index() {
        dataService.execute("{'startTime': '2020-01-01 00:00:00','endTime': '2020-02-01 00:00:00'}");
    }
}
