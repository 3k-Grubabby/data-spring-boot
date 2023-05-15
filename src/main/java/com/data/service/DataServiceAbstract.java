package com.data.service;

import cn.hutool.core.date.DateTime;
import com.data.dao.CommonDao;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.supconsoft.plantwrapex.IConnection;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Getter
@Setter
public abstract class DataServiceAbstract implements DataService {
    protected List<String> floatTags = new ArrayList<>();
    protected List<String> stringTags = new ArrayList<>();
    protected AtomicReference<DateTime> startTime = new AtomicReference<>(null);
    protected AtomicReference<DateTime> endTime = new AtomicReference<>(null);
    protected Integer offset = 299000;
    protected Integer interval = 300;
    protected Integer step = 183;
    protected String groupName = "-1";

    @Autowired
    protected CommonDao commonDao;

    private static final String TAGKEY = "SAMPLEPOINT";


    private static final String LASTTIME_TABLE = "esp_lasttime_table";
    private static final String TAGNAME = "tagname";
    private static final String LASTTIME = "lasttime";

    protected IConnection espConnection;
    protected String odsConnection;

    /**
     * 根据组名获取tag列表
     */
    public void getTagListByGroupName() {
        Map<String, Object> conditions = !groupName.equalsIgnoreCase("-1")
                ? ImmutableMap.of("TABLENAME", groupName)
                : Collections.emptyMap();
        List<Map<String, Object>> configTable = commonDao.selectRecordsWithConditions("configtable", conditions);
        Preconditions.checkArgument(configTable.size() > 0, "未找到该组名");

        configTable.forEach(
                config -> {
                    Optional.ofNullable(config.get("DATATYPE"))
                            .filter(o -> !config.isEmpty())
                            .map(Object::toString)
                            .ifPresent(dataType -> {
                                if (dataType.equalsIgnoreCase("float") || dataType.equalsIgnoreCase("double")) {
                                    Optional.ofNullable(config.get(TAGKEY)).ifPresent(tagname -> {
                                        floatTags.add(tagname.toString());
                                    });
                                } else if (dataType.equalsIgnoreCase("string")) {
                                    Optional.ofNullable(config.get(TAGKEY)).ifPresent(tagname -> {
                                        stringTags.add(tagname.toString());
                                    });
                                }
                            });
                }
        );
    }

    /**
     * 获取tag的最后更新时间
     *
     * @param tagName
     * @return
     */
    protected Date getTagLastTime(String tagName) {
        HashMap<String, Object> conditions = new HashMap<>();
        conditions.put(TAGNAME, tagName);
        List<Map<String, Object>> lasttimeTable = commonDao.selectRecordsWithConditions(LASTTIME_TABLE, conditions);

        return Optional.ofNullable(lasttimeTable)
                .filter(list -> !list.isEmpty())
                .map(list -> list.get(0))
                .map(record -> {
                    LocalDateTime localDateTime = (LocalDateTime) record.get(LASTTIME);
                    Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
                    return Date.from(instant);
                })
                .orElse(null);
    }
    /*
     更新tag的最后更新时间
     */
    protected void updateTagLastTime(String tagName, Date lastTime) {
        HashMap<String, Object> conditions = new HashMap<>();
        conditions.put(TAGNAME, tagName);
        List<Map<String, Object>> lasttimeTable = commonDao.selectRecordsWithConditions(LASTTIME_TABLE, conditions);

        if (lasttimeTable.isEmpty()) {
            HashMap<String, Object> record = new HashMap<>();
            record.put(TAGNAME, tagName);
            record.put(LASTTIME, lastTime);
            commonDao.insertRecords(LASTTIME_TABLE, Collections.singletonList(record));
        } else {
            HashMap<String, Object> record = new HashMap<>();
            record.put(TAGNAME, tagName);
            record.put(LASTTIME, lastTime);
            commonDao.updateRecords(LASTTIME_TABLE, record, conditions);
        }
    }
}
