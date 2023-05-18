package com.data.service.impl;

import com.data.service.abstracts.EspServiceAbstract;
import com.supconsoft.plantwrapex.IConnection;
import com.supconsoft.plantwrapex.PlantWrapException;
import com.supconsoft.plantwrapex.TagValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service("realTimeDataService")
public class RealTimeDataServiceImpl extends EspServiceAbstract {
    private IConnection connection;
    private List<Map<String, Object>> configTable;

    @Value("${data.esp.url:null}")
    protected String url;

    @Override
    public void execute(String params) {
        // 连接实时数据库
        connection = getConnection(url);
        // 清空实时表
        clearRealTimeTable("float");
        clearRealTimeTable("string");

        // 如果configTable 为空，获取tag列表
        configTable = Optional.ofNullable(configTable).orElseGet(this::getTagListByGroupName);

        configTable.forEach(
                config -> {
                    Optional.ofNullable(config.get(DATATYPE))
                            .filter(o -> !config.isEmpty())
                            .map(Object::toString)
                            .ifPresent(dataType -> {
                                if (dataType.equalsIgnoreCase("float") || dataType.equalsIgnoreCase("double")) {
                                    Optional.ofNullable(config.get(TAGKEY)).ifPresent(tagname -> {
                                        save(tagname.toString(), "float");
                                    });
                                } else if (dataType.equalsIgnoreCase("string")) {
                                    Optional.ofNullable(config.get(TAGKEY)).ifPresent(tagname -> {
                                        save(tagname.toString(), "string");
                                    });
                                }
                            });

                }
        );
        // 关闭连接
        connection.close();
    }

    /**
     * 保存数据
     * @param tagName 标签名
     * @param dataType 数据类型
     */
    public void save(String tagName, String dataType) {
        try {
            TagValue tagValue = connection.getTagFactory().findTag(tagName).readValue();
            Optional.ofNullable(tagValue).ifPresent( value -> {
                // 将数据转换为List<Map<String, Object>>格式
                List<Map<String, Object>> records = convertToRecords(new TagValue[]{value}, tagName, dataType);
                if(!records.isEmpty()){
                    // 插入数据
                    insertRealTimeRecords(records, dataType);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
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
