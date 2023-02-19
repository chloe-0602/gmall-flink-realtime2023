package com.cherry.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cherry.gmall.realtime.bean.TableProcess;
import com.cherry.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection conn;
    MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties props = new Properties();
        props.setProperty("phoenix.schema.isNamespaceMappingEnabled","true");
        props.setProperty("phoenix.schema.mapSystemTablesToNamespace","true");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER, props);
    }

    // 先处理广播流数据
    // value:{"before":null,"after":{"source_table":"aa","sink_table":"bb","sink_columns":"cc","sink_pk":"id","sink_extend":"xxx"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1652513039549,"snapshot":"false","db":"gmall-211126-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1652513039551,"transaction":null}
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        // 1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        // 2.校验表是否存在，不存在则建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());
        // 3.写出状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        // 1.获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObject.getString("table");
        TableProcess tableProcess = broadcastState.get(table);

        if(tableProcess != null){
            // 2.过滤字段
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());
            // 3.补充sinkTable并写出到流
            jsonObject.put("sinkTable", tableProcess.getSinkTable());
            collector.collect(jsonObject);
        }else{
            System.out.println("在gmall_config.table_process 找不到对应的table(不是维表或者我们不需要)： " + table);
        }
    }

    /**
     * 校验并建表，用于处理广播流的第二步
     * create table if not exists db.tn(id varchar primary key,bb varchar,cc varchar) xxx
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
            if(sinkPk == null || "".equals(sinkPk)){
                sinkPk = "id";
            }
            if(sinkExtend == null || "".equals(sinkExtend)){
                sinkExtend = "";
            }

            StringBuilder createTabSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if(sinkPk.equals(column)){
                    createTabSql.append(column)
                            .append(" varchar primary key");
                }else {
                    createTabSql.append(column)
                            .append(" varchar");
                }

                if(i < columns.length - 1){
                    createTabSql.append(",");
                }
            }

            createTabSql.append(")")
                    .append(sinkExtend);

            System.out.println(" Phoenix建表语句为： " + createTabSql);

            preparedStatement = conn.prepareStatement(createTabSql.toString());
            preparedStatement.execute();

//            String testSql = " create schema GMALL2023_REALTIME";
//            System.out.println(" test Phoenix语句为： " + testSql);
//            preparedStatement = conn.prepareStatement(testSql);

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败：" + sinkTable);
        }finally {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 过滤字段，用于主流的第二步
     * @param data
     * @param sinkColumns
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] conlumns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(conlumns);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }
}
