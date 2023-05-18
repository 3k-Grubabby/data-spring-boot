package com.data.service.abstracts;

import cn.hutool.core.date.DateTime;
import com.data.dao.CommonDao;
import com.data.service.DataService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.supconsoft.plantwrapex.IConnection;
import com.supconsoft.plantwrapex.TagValue;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@Setter
public abstract class DataServiceAbstract implements DataService<IConnection> {
    protected AtomicReference<DateTime> startTime = new AtomicReference<>(null);
    protected AtomicReference<DateTime> endTime = new AtomicReference<>(null);
    protected Integer offset = 299000;
    protected Integer interval = 300;
    protected Integer step = 366;
    protected String groupName = "-1";

    @Autowired
    protected CommonDao commonDao;

    protected static final String DATATYPE = "DATATYPE";
    protected static final String TAGKEY = "SAMPLEPOINT";

    private static final String LASTTIME_TABLE = "esp_lasttime_table";
    private static final String TAGNAME = "tagname";
    private static final String LASTTIME = "lasttime";

    public DataServiceAbstract() {
    }


    /**
     * 根据组名获取tag列表
     */
    public List<Map<String, Object>> getTagListByGroupName() {
        Map<String, Object> conditions = !groupName.equalsIgnoreCase("-1")
                ? ImmutableMap.of("TABLENAME", groupName)
                : Collections.emptyMap();
        List<Map<String, Object>> configTable = commonDao.selectRecordsWithConditions("configtable", conditions);
        Preconditions.checkArgument(configTable.size() > 0, "未找到该组名");
        return configTable;
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

    protected List<Map<String, Object>> convertToRecords(TagValue[] tagValues, String tagname, String datatype) {
        try{
            List<Map<String, Object>> collect = Stream.of(tagValues)
                    .map(tagValue -> {
                        Map<String, Object> record = new HashMap<>();
                        record.put("TAGNAME", tagname);
                        Optional.ofNullable(tagValue.getValue()).ifPresentOrElse(
                                value -> record.put("TAGVALUE", "string".equalsIgnoreCase(datatype) ? tagValue.getValue().toString() : Float.parseFloat(tagValue.getValue().toString())),
                                () -> record.put("TAGVALUE", "string".equalsIgnoreCase(datatype) ? null : 0)
                        );
                        record.put("TAGTIME", tagValue.getTimeStamp());
                        return record;
                    }).collect(Collectors.toList());
            return collect;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    protected void insertHistoryRecords(List<Map<String, Object>> records, String datatype) {
        String tableName = "float".equalsIgnoreCase(datatype) ? "prochisttable_float" : "prochisttable_string";
        commonDao.insertRecords(tableName, records);
    }
    protected void insertRealTimeRecords(List<Map<String, Object>> records, String datatype) {
        String tableName = "float".equalsIgnoreCase(datatype) ? "procrttable_float" : "procrttable_string";
        commonDao.insertRecords(tableName, records);
    }
    /**
     * 清空实时表
     */
    protected void clearRealTimeTable(String datatype) {
        String tableName = "float".equalsIgnoreCase(datatype) ? "procrttable_float" : "procrttable_string";
        commonDao.truncateTable(tableName);
    }

}
