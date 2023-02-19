package com.cherry.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.cherry.gmall.realtime.until.DruidDSUtil;
import com.cherry.gmall.realtime.until.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    DruidDataSource druidDataSource = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    //value:{"database":"gmall-211126-flink","table":"base_trademark","type":"update","ts":1652499176,"xid":188,"commit":true,"data":{"id":13,"tm_name":"atguigu"},"old":{"logo_url":"/aaa/aaa"},"sinkTable":"dim_xxx"}

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 1.获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");

        // 2.获取数据类型
//        String type = value.getString("type");

        // 3.写出数据
        PhoenixUtil.upsertValues(connection, sinkTable, data);

        // 4.归还连接
        connection.close();
    }
}
