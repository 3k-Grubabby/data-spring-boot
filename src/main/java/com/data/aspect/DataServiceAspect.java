package com.data.aspect;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.data.service.DataServiceAbstract;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@Aspect
@Component
public class DataServiceAspect {

    @Value("${data.esp.url:null}")
    private String espUrl;

    @Value("${data.ods.url:null}")
    private String odsUrl;


    @Pointcut("execution(* com.data.service.DataService.execute(String)) && args(params)")
    public void executePointcut(String params) {
    }

    @Around("executePointcut(params)")
    public void beforeExecute(ProceedingJoinPoint joinPoint, String params) throws Throwable {

        // 访问代理对象
        DataServiceAbstract proxy = (DataServiceAbstract) joinPoint.getThis();
        //params是json，将它转为为map
        Map<String, Object> paramsMap = JSON.parseObject(params, Map.class);

        Optional.ofNullable(paramsMap.get("startTime")).ifPresent(startTime -> {
            proxy.setStartTime(new AtomicReference<>(DateUtil.parseDateTime(startTime.toString())));
        });

        Optional.ofNullable(paramsMap.get("endTime")).ifPresentOrElse(endTime -> {
            proxy.setEndTime(new AtomicReference<>(DateUtil.parseDateTime(endTime.toString())));
        }, () -> {
            proxy.setEndTime(new AtomicReference<>(DateUtil.date()));
        });

        Optional.ofNullable(paramsMap.get("offset")).ifPresent(offset -> {
            proxy.setOffset(Integer.parseInt(offset.toString()));
        });
        Optional.ofNullable(paramsMap.get("interval")).ifPresent(interval -> {
            proxy.setInterval(Integer.parseInt(interval.toString()));
        });
        Optional.ofNullable(paramsMap.get("groupName")).ifPresent(groupName -> {
            proxy.setGroupName(groupName.toString());
        });

        // 清空floatTags 和 stringTags
        proxy.getFloatTags().clear();
        proxy.getStringTags().clear();

        proxy.getTagListByGroupName();

        Optional.ofNullable(espUrl).ifPresent(proxy::conn);
        Optional.ofNullable(odsUrl).ifPresent(proxy::conn);

        // 调用目标方法，并传递修改后的参数
        Object result = joinPoint.proceed();

        proxy.close();
    }
}
