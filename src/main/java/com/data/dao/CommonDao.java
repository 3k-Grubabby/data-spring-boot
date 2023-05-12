package com.data.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class CommonDao {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> selectRecordsWithConditions(String tableName, Map<String, Object> conditions) {
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(tableName).append(" WHERE 1 = 1 ");
        List<Object> args = new ArrayList<>();
        conditions.forEach((key, value) -> {
            sql.append(" AND ").append(key).append(" = ? ");
            args.add(value);
        });
        return jdbcTemplate.queryForList(sql.toString(), args.toArray());
    }
    public int[] insertRecords(String tableName, List<Map<String, Object>> records) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName);
        sql.append(" (");
        Set<String> columnNames = records.get(0).keySet();
        sql.append(String.join(",", columnNames));
        sql.append(") VALUES ( ");

        List<String> placeholders = Collections.nCopies(columnNames.size(), "?");
        sql.append(String.join(",", placeholders));
        sql.append(")");

        List<Object[]> batchArgs = new ArrayList<>();
        for (Map<String, Object> record : records) {
            Object[] args = new Object[columnNames.size()];
            int i = 0;
            for (String columnName : columnNames) {
                args[i++] = record.get(columnName);
            }
            batchArgs.add(args);
        }
        return jdbcTemplate.batchUpdate(sql.toString(), batchArgs);
    }
}
