package com.data.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class CommonDao {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 查询数据
     *
     * @param tableName  表名
     * @param conditions 查询条件
     * @return 查询结果
     */
    public List<Map<String, Object>> selectRecordsWithConditions(String tableName, Map<String, Object> conditions) {
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(tableName).append(" WHERE 1 = 1 ");
        List<Object> args = new ArrayList<>();
        conditions.forEach((key, value) -> {
            sql.append(" AND ").append(key).append(" = ? ");
            args.add(value);
        });
        return jdbcTemplate.queryForList(sql.toString(), args.toArray());
    }

    /**
     * 插入数据
     *
     * @param tableName 表名
     * @param records   插入数据
     */
    public void insertRecords(String tableName, List<Map<String, Object>> records) {
        //线程数
        int numThreads = 4;
        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        // 每次提交的数据量
        int batchSize = 1000;
        // 拼接 sql
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName);
        sql.append(" (");
        Set<String> columnNames = records.get(0).keySet();
        sql.append(String.join(",", columnNames));
        sql.append(") VALUES ( ");
        List<String> placeholders = Collections.nCopies(columnNames.size(), "?");
        sql.append(String.join(",", placeholders));
        sql.append(")");

        // 分批提交 1000 条数据
        for (int i = 0; i < records.size(); i += batchSize) {
            final List<Map<String, Object>> batchList = records.subList(i, Math.min(i + batchSize, records.size()));
            executor.submit(() -> {
                List<Object[]> batchArgs = new ArrayList<>();
                for (Map<String, Object> record : batchList) {
                    Object[] args = new Object[columnNames.size()];
                    int j = 0;
                    for (String columnName : columnNames) {
                        args[j++] = record.get(columnName);
                    }
                    batchArgs.add(args);
                }
                try {
                    jdbcTemplate.batchUpdate(sql.toString(), batchArgs);
                } catch (Exception e) {
                   throw new RuntimeException(e);
                }
            });
        }
        executor.shutdown();
        try {
            // 等待所有任务都执行结束
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 更新数据
     *
     * @param tableName  表名
     * @param record     更新数据
     * @param conditions 更新条件
     */
    public void updateRecords(String tableName, HashMap<String, Object> record, HashMap<String, Object> conditions) {
        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        List<Object> args = new ArrayList<>();
        record.forEach((key, value) -> {
            sql.append(key).append(" = ? ,");
            args.add(value);
        });
        sql.deleteCharAt(sql.length() - 1); // 删除最后一个逗号分隔符
        sql.append(" WHERE 1 = 1 ");
        conditions.forEach((key, value) -> {
            sql.append(" AND ").append(key).append(" = ? ");
            args.add(value);
        });
        jdbcTemplate.update(sql.toString(), args.toArray());
    }

    /**
     * 清空表数据
     *
     * @param tableName 表名
     */
    public void truncateTable(String tableName) {
        String sql = "TRUNCATE TABLE " + tableName;
        jdbcTemplate.execute(sql);
    }
}
