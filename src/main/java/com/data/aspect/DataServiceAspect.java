package com.data.aspect;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.data.service.abstracts.DataServiceAbstract;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@Aspect
@Component
public class DataServiceAspect {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataServiceAspect.class);

    private static final String START_TIME = "startTime";
    private static final String END_TIME = "endTime";
    private static final String OFFSET = "offset";
    private static final String INTERVAL = "interval";
    private static final String GROUP_NAME = "groupName";

    @Pointcut("execution(* com.data.service.DataService.execute(String)) && args(params)")
    public void executePointcut(String params) {
    }

    @Before("executePointcut(params)")
    public void beforeExecute(JoinPoint joinPoint, String params) throws Throwable {
        // 访问代理对象
        DataServiceAbstract proxy = (DataServiceAbstract) joinPoint.getThis();
        //params是json，将它转为为map
        Map<String, Object> paramsMap = JSON.parseObject(params, Map.class);
        // 设置代理对象的参数
        setProxyParams(proxy, paramsMap);
    }

    private void setProxyParams(DataServiceAbstract proxy, Map<String, Object> paramsMap) {
        Optional.ofNullable(paramsMap.get(START_TIME)).ifPresent(startTime -> proxy.setStartTime(new AtomicReference<>(DateUtil.parseDateTime(startTime.toString()))));
        Optional.ofNullable(paramsMap.get(END_TIME)).ifPresent(endTime -> proxy.setEndTime(new AtomicReference<>(DateUtil.parseDateTime(endTime.toString()))));
        Optional.ofNullable(paramsMap.get(OFFSET)).ifPresent(offset -> proxy.setOffset(Integer.parseInt(offset.toString())));
        Optional.ofNullable(paramsMap.get(INTERVAL)).ifPresent(interval -> proxy.setInterval(Integer.parseInt(interval.toString())));
        Optional.ofNullable(paramsMap.get(GROUP_NAME)).ifPresent(groupName -> proxy.setGroupName(groupName.toString()));
    }
}
