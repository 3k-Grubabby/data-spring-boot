package com.data.service.impl;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.data.service.abstracts.EspServiceAbstract;
import com.supconsoft.plantwrapex.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Service("historyDataService")
public class HistoryDataServiceImpl extends EspServiceAbstract {

    @Value("${data.esp.url:null}")
    protected String url;

    private IConnection connection;

    private List<Map<String, Object>> configTable;

    //创建一个队列，用于存放请求失败的tagname
    private final Queue<HashMap<String, String>> queue = new LinkedList<>();

    @Override
    public void execute(String params) {
        // 如果configTable 为空，获取tag列表
        configTable = Optional.ofNullable(configTable).orElseGet(this::getTagListByGroupName);
        //循环queue，请求插入上次失败的tagname
        while (!queue.isEmpty()) {
            // 连接实时数据库
            connection = getConnection(url);

            HashMap<String, String> poll = queue.poll();
            if (poll.get("dataType").equalsIgnoreCase("float") || poll.get("dataType").equalsIgnoreCase("double")) {
                save(poll.get("tagName"), startTime, endTime, offset, interval, "float");
            } else if (poll.get("dataType").equalsIgnoreCase("string")) {
                save(poll.get("tagName"), startTime, endTime, offset, interval, "string");
            }
            // 关闭连接
            connection.close();
        }

        configTable.forEach(
                config -> {
                    // 连接实时数据库
                    connection = getConnection(url);
                    Optional.ofNullable(config.get(DATATYPE))
                            .filter(o -> !config.isEmpty())
                            .map(Object::toString)
                            .ifPresent(dataType -> {
                                if (dataType.equalsIgnoreCase("float") || dataType.equalsIgnoreCase("double")) {
                                    Optional.ofNullable(config.get(TAGKEY)).ifPresent(tagname -> {
                                        save(tagname.toString(), startTime, endTime, offset, interval, "float");
                                    });
                                } else if (dataType.equalsIgnoreCase("string")) {
                                    Optional.ofNullable(config.get(TAGKEY)).ifPresent(tagname -> {
                                        save(tagname.toString(), startTime, endTime, offset, interval, "string");
                                    });
                                }
                            });
                    // 关闭连接
                    connection.close();
                }
        );
    }

    public void save(String tagname, AtomicReference<DateTime> startTime, AtomicReference<DateTime> endTime, Integer offset, Integer interval, String dataType) {
        try {
            // 从数据库获取最后一条数据的时间
            Optional<Date> tagLastTimeOpt = Optional.ofNullable(getTagLastTime(tagname));
            //如果有tagLastTime,则startTime为tagLastTime加上interval
            tagLastTimeOpt.ifPresent(tagLastTime -> startTime.set(DateUtil.offsetSecond(DateTime.of(tagLastTime), interval)));

            DateTime currentStartTime = startTime.get();
            DateTime currentEndTime = DateUtil.offsetDay(currentStartTime, step);
            DateTime endDateTime = endTime.get();

            // 判断当前开始时间是否大于传递过来的结束时间
            while (currentStartTime.isBefore(endDateTime)) {

                // 如果结束时间大于设定的结束时间，则将结束时间设置为设定的结束时间
                if (currentEndTime.isAfter(endDateTime)) {
                    currentEndTime = endDateTime;
                }
                // 请求接口数据
                TagValue[] tagValues = connection.getTagFactory().findTag(tagname).readHisValue(currentStartTime, currentEndTime, SampleOption.Sample_Before, offset, interval);
                // 将数据转换为List<Map<String, Object>>格式
                List<Map<String, Object>> records = convertToRecords(tagValues, tagname, dataType);
                if (!records.isEmpty()) {
                    // 插入数据
                    insertHistoryRecords(records, dataType);
                    // 更新最后一条数据的时间
                    updateTagLastTime(tagname, currentEndTime);
                }
                // 更新当前开始时间为当前结束时间+interval，进入下一个时间段
                currentStartTime = DateUtil.offsetSecond(currentEndTime, interval);
                currentEndTime = DateUtil.offsetDay(currentStartTime, step);
            }
        } catch (Exception e) {
            // 如果请求失败，将tagname加入队列
            HashMap<String, String> errorTagNameHashMap = new HashMap<>();
            errorTagNameHashMap.put("tagName", tagname);
            errorTagNameHashMap.put("dataType", dataType);
            queue.offer(errorTagNameHashMap);
            e.printStackTrace();// 打印异常信息
        }
    }

    @PreDestroy
    @Override
    public void destroy() {
        if (connection != null) {
            connection.close();
        }
    }

}
