package com.data.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

@Mapper
public interface CommonMapper extends BaseMapper<Object> {
    /**
     * 查询指定表中的所有记录
     *
     * @param tableName 表名
     * @return 包含所有记录的列表
     */
    @Select("<script>" +
            "SELECT * FROM ${tableName}" +
            "<where>" +
            "<if test='conditions != null and !conditions.isEmpty()'>" +
            "  <foreach collection='conditions' index='key' item='value' separator=' AND '>" +
            "    ${key} = #{value}" +
            "  </foreach>" +
            "</if>" +
            "</where>" +
            "</script>")
    List<Map<String, Object>> selectRecordsWithConditions(@Param("tableName") String tableName,
                                                          @Param("conditions") Map<String, Object> conditions);


    /**
     * 向指定表中批量插入多条记录
     *
     * @param tableName 表名
     * @param records   待插入的记录列表
     * @return 影响的行数
     */
    @Insert("<script>" +
            "INSERT INTO ${tableName} (${keys}) VALUES " +
            "<foreach collection='records' item='record' separator=','>" +
            "(<foreach collection='record.values' item='value' separator=','>#{value}</foreach>)" +
            "</foreach>" +
            "</script>")
    int insertRecords(@Param("tableName") String tableName,
                      @Param("keys") String keys,
                      @Param("records") List<Map<String, Object>> records);


}
