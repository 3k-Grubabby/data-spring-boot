package com.data.service.impl;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.data.manager.impl.EspConnectionImpl;
import com.data.service.DataServiceAbstract;
import com.supconsoft.plantwrapex.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class DataServiceImpl extends DataServiceAbstract {

    @Override
    public void execute(String params) {

        // 伪造100条数据,调用CommonDao的insert方法，将数据插入到数据库中
//        List<Map<String, Object>> list = new ArrayList<>();
//        for (int i = 0; i < 500000; i++) {
//            Map<String, Object> map = new HashMap<>();
//            map.put("tagname", "tagname" + i);
//            map.put("tagvalue", i);
//            map.put("tagtime", new Date());
//            list.add(map);
//        }
//        //计算插入数据的时间
//        long start = System.currentTimeMillis();
//        commonDao.insertRecords("prochisttable_float", list);
//        long end = System.currentTimeMillis();
//        System.out.println("插入数据的时间为:" + (end - start) + "ms");


//        System.out.println(stringTags.size());

//        String[] array = floatTags.toArray(new String[0]);
//        try {
//            TagValueRow[] tagValueRows = espConnection.getTagFactory().getTagGroup(array).readHisValue(DateTime.of(Long.parseLong("2020-01-01 00:00:00")), DateTime.of(Long.parseLong("2021-01-01 00:00:10")), SampleOption.Sample_Before, offset, interval);
//            //将tagValueRows转为list
//            List<TagValueRow> tagValueRowList = Arrays.asList(tagValueRows);
//            tagValueRowList.forEach(tagValueRow -> {
//                TagValue[] values = tagValueRow.getValues();
//                System.out.println(values.length);
//            });
//        } catch (PlantWrapException e) {
//            e.printStackTrace();
//        }
//



//        floatTags.forEach(floatTag -> {
//            save(floatTag, startTime, endTime,offset,interval,"float");
//        });
//        stringTags.forEach(stringTag -> {
//            save(stringTag, startTime, endTime,offset,interval,"string");
//        });
    }

    public void save(String tagname, AtomicReference<DateTime> startTime, AtomicReference<DateTime> endTime, Integer offset, Integer interval, String datatype) {
        // 从数据库获取最后一条数据的时间
        Date tagLastTime = getTagLastTime(tagname);
        if (tagLastTime != null) {
            //如果有tagLastTime,则startTime为tagLastTime加上interval
            startTime.set(DateUtil.offsetSecond(DateTime.of(tagLastTime), interval));
        }

        DateTime currentStartTime = startTime.get();
        DateTime currentEndTime;

        // 判断当前开始时间是否大于传递过来的结束时间
        while (currentStartTime.isBefore(endTime.get())) {

            // 当前开始时间加上step 作为结束时间
            currentEndTime = DateUtil.offsetDay(currentStartTime, step);

            // 如果结束时间大于设定的结束时间，则将结束时间设置为设定的结束时间
            if (currentEndTime.isAfter(endTime.get())) {
                currentEndTime = endTime.get();
            }

            // 请求接口数据
            try {
                TagValue[] tagValues = espConnection.getTagFactory().findTag(tagname).readHisValue(currentStartTime, currentEndTime, SampleOption.Sample_Before, offset, interval);
                // 将数据转换为List<Map<String, Object>>格式
                List<Map<String, Object>> records = Stream.of(tagValues)
                        .map(tagValue -> {
                            Map<String, Object> record = new HashMap<>();
                            record.put("TAGNAME", tagname);
                            record.put("TAGVALUE", "string".equalsIgnoreCase(datatype) ? tagValue.getValue().toString() : Float.parseFloat(tagValue.getValue().toString()));
                            record.put("TAGTIME", tagValue.getTimeStamp());
                            return record;
                        }).collect(Collectors.toList());

                String tableName = "float".equalsIgnoreCase(datatype) ? "prochisttable_float" : "prochisttable_string";

                if (records.size() > 0) {
                    // 插入数据
                    commonDao.insertRecords(tableName, records);
                    // 更新最后一条数据的时间
                    updateTagLastTime(tagname, currentEndTime);

                }

            } catch (PlantWrapException e) {
                e.printStackTrace();
            }

            // 更新当前开始时间为当前结束时间+interval，进入下一个时间段
            currentStartTime = DateUtil.offsetSecond( currentEndTime,interval);

        }
    }
}
