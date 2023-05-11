package com.data.service;

import cn.hutool.core.date.DateTime;
import com.data.mapper.CommonMapper;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.apache.tomcat.util.buf.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Getter
@Setter
public abstract class DataServiceAbstract implements DataService {
    protected List<String> floatTags = new ArrayList<>();
    protected List<String> stringTags = new ArrayList<>();
    protected AtomicReference<DateTime> startTime = new AtomicReference<>(null);
    protected AtomicReference<DateTime> endTime = new AtomicReference<>(null);
    protected Integer offset = 0;
    protected Integer interval = 300;
    protected Integer step = 90;
    protected String groupName = "-1";

    @Autowired
    protected CommonMapper commonMapper;

    private String tagKey = "SAMPLEPOINT";


    {
        System.out.println("DataServiceAbstract init");
    }

    public void getTagListByGroupName() {
        HashMap<String, Object> objectObjectHashMap = new HashMap<>();
        if (!groupName.equalsIgnoreCase("-1")) {
            objectObjectHashMap.put("TABLENAME", groupName);
        }
        List<Map<String, Object>> configtable = commonMapper.selectRecordsWithConditions("configtable", objectObjectHashMap);
        Preconditions.checkArgument(configtable.size() > 0, "未找到该组名");
        configtable.forEach(
                config -> {
                    if (!config.isEmpty()) {
                        Optional<Object> datatype = Optional.ofNullable(config.get("DATATYPE"));
                        datatype.ifPresent(o -> {

                            if (o.toString().equalsIgnoreCase("float") || o.toString().equalsIgnoreCase("double")) {
                                Optional.ofNullable(config.get(tagKey)).ifPresent(tagname -> {
                                    floatTags.add(tagname.toString());
                                });
                            } else if (o.toString().equalsIgnoreCase("string")) {
                                Optional.ofNullable(config.get(tagKey)).ifPresent(tagname -> {
                                    stringTags.add(tagname.toString());
                                });
                            }
                        });
                    }
                }
        );
    }

    protected int insertRecords(String tableName, List<Map<String, Object>> records) {
        if (records.isEmpty()) {
            return 0;
        }
        String keys = StringUtils.join(records.get(0).keySet(), ',');
        return commonMapper.insertRecords(tableName, keys, records);
    }

    protected Date getTagLastTime(String tagName) {
        HashMap<String, Object> objectObjectHashMap = new HashMap<>();
        objectObjectHashMap.put("tagname", tagName);
        List<Map<String, Object>> lasttimeTable = commonMapper.selectRecordsWithConditions("esp_lasttime_table", objectObjectHashMap);
        List<Map<String, Object>> espLasttimeTableIsNull = Preconditions.checkNotNull(lasttimeTable, "esp_lasttime_table is null");
        Map<String, Object> stringObjectMap = espLasttimeTableIsNull.get(0);
        Date lasttime = (Date) stringObjectMap.get("lasttime");
        return lasttime;
    }
}
