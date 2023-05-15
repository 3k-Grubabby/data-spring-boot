package com.data.aspect;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.data.manager.impl.EspConnectionImpl;
import com.data.manager.impl.OdsConnectionImpl;
import com.data.service.DataServiceAbstract;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@Aspect
@Component
public class DataServiceAspect {

    @Value("${data.esp.url:null}")
    private String espUrl;

    @Value("${data.ods.url:null}")
    private String odsUrl;

    @Autowired
    private EspConnectionImpl espConnection;
    @Autowired
    private OdsConnectionImpl odsConnection;


    @Pointcut("execution(* com.data.service.DataService.execute(String)) && args(params)")
    public void executePointcut(String params) {
    }

    @Around("executePointcut(params)")
    public void beforeExecute(ProceedingJoinPoint joinPoint, String params) throws Throwable {

        // 访问代理对象
        DataServiceAbstract proxy = (DataServiceAbstract) joinPoint.getThis();
        //params是json，将它转为为map
        Map<String, Object> paramsMap = JSON.parseObject(params, Map.class);

        setProxyParams(proxy, paramsMap);

        proxy.getTagListByGroupName();

        // 建立连接
//        establishConnections(proxy);

        //计算插入数据的时间
        long start = System.currentTimeMillis();

        Object result = joinPoint.proceed();
        //计算插入数据的时间
        long end = System.currentTimeMillis();
        System.out.println("插入数据的时间为:" + (end - start) + "ms");
        // 关闭连接
//        establishCloses(proxy);

    }

    private void setProxyParams(DataServiceAbstract proxy, Map<String, Object> paramsMap) {
        Optional.ofNullable(paramsMap.get("startTime")).ifPresent(startTime -> proxy.setStartTime(new AtomicReference<>(DateUtil.parseDateTime(startTime.toString()))));
        Optional.ofNullable(paramsMap.get("endTime")).ifPresent(endTime -> proxy.setEndTime(new AtomicReference<>(DateUtil.parseDateTime(endTime.toString()))));
        Optional.ofNullable(paramsMap.get("offset")).ifPresent(offset -> proxy.setOffset(Integer.parseInt(offset.toString())));
        Optional.ofNullable(paramsMap.get("interval")).ifPresent(interval -> proxy.setInterval(Integer.parseInt(interval.toString())));
        Optional.ofNullable(paramsMap.get("groupName")).ifPresent(groupName -> proxy.setGroupName(groupName.toString()));

        proxy.getFloatTags().clear();
        proxy.getStringTags().clear();
    }

    private void establishConnections(DataServiceAbstract proxy) {
        if (espUrl != null) {
            espConnection.connect(espUrl);
            proxy.setEspConnection(espConnection.getConnection());
        }
        if (odsUrl != null) {
            odsConnection.connect(odsUrl);
            proxy.setOdsConnection(odsConnection.getConnection());
        }
    }

    @PreDestroy
    private void establishCloses() {
        if (espUrl != null) {
            espConnection.close();
        }
        if (odsUrl != null) {
            odsConnection.close();
        }
    }

}
