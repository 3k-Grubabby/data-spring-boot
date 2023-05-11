package com.data.service.impl;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.data.service.DataServiceAbstract;
import com.supconsoft.plantwrapex.*;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class DataServiceImpl extends DataServiceAbstract {

    private IConnection connection;


    @Override
    public void execute(String params) {

        floatTags.forEach(floatTag -> {
            save(floatTag, startTime, endTime, interval, offset, "float");
        });
        stringTags.forEach(stringTag -> {
            save(stringTag, startTime, endTime, interval, offset, "string");
        });
    }

    public void save(String tagname, AtomicReference<DateTime> startTime, AtomicReference<DateTime> endTime, Integer offset, Integer interval, String datatype) {
        // 从数据库获取最后一条数据的时间
        Date tagLastTime = getTagLastTime(tagname);
        //如果有tagLastTime,则startTime为tagLastTime加上interval
        Optional.ofNullable(tagLastTime).ifPresent(t -> {
            startTime.set(DateUtil.offsetSecond(DateTime.of(t), interval));
        });
        DateTime currentStartTime = startTime.get();
        DateTime currentEndTime = endTime.get();

        while (currentStartTime.isBefore(endTime.get())) {
            // 开始时间加上step 作为结束时间
            currentEndTime = DateUtil.offsetDay(currentStartTime, step);

            if(currentEndTime.isBefore(endTime.get())){
                currentEndTime = endTime.get();
            }
            // 判断结束时间是否大于结束时间
            if (currentEndTime.isAfter(endTime.get())) {
                currentEndTime = endTime.get();
            }

            Optional.ofNullable(tagLastTime).ifPresent(t -> {
                DateTime lastTagtime = DateTime.of(t);
                // 最后一条数据的时间加上interval 作为开始时间
                startTime.set(DateUtil.offsetSecond(lastTagtime, interval));
            });

            // 请求接口数据
            try {
                TagValue[] tagValues = connection.getTagFactory().findTag(tagname).readHisValue(startTime.get(), currentEndTime, SampleOption.Sample_Before, offset, interval);

                List<Map<String, Object>> records = new ArrayList<>();

                for (TagValue tagValue : tagValues) {
                    Map<String, Object> record = new HashMap<>();
                    record.put("TAGNAME", tagname);
                    record.put("TAGVALUE", tagValue.getValue().toString());
                    record.put("TAGTIME", tagValue.getTimeStamp().toString());
                    records.add(record);
                }
                if (datatype.equalsIgnoreCase("float")) {
                    // 插入数据库
                    int rows = insertRecords("prochisttable_float", records);
                } else if (datatype.equalsIgnoreCase("string")) {
                    // 插入数据库
                    int rows = insertRecords("prochisttable_string", records);
                }
            } catch (PlantWrapException e) {
                e.printStackTrace();
            }

        }
    }


    @Override
    public void conn(String url) {
        System.out.println("conn"+url);
        try {
            connection = ConnectionCreator.getConnection(url);
        } catch (PlantWrapException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        System.out.println("close connection");
        connection.close();
    }
}
