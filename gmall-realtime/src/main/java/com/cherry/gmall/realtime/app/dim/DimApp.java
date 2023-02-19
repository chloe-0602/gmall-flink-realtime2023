package com.cherry.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.cherry.gmall.realtime.app.func.DimSinkFunction;
import com.cherry.gmall.realtime.app.func.TableProcessFunction;
import com.cherry.gmall.realtime.bean.TableProcess;
import com.cherry.gmall.realtime.until.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson.JSONObject;

public class DimApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 生产环境设置为kafka主题的分区数

        // 1.1 开启ck
//        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout( 10 * 60000L );
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        // 1.2 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/230214/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 2.读取 Kafka topic_db 主题数据创建主流
        String topic = "topic_db";
        String groupId = "dim_app";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //kafkaDS.print("########## kafkaDS #############");

        // TODO 3.过滤掉非JSON数据&保留新增、变化以及初始化数据并将数据转换为JSON格式
        SingleOutputStreamOperator<JSONObject> filterJsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                try {

                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");

                    if("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)){
                        collector.collect(jsonObject);
                    }

                }catch (Exception e){
                    System.out.println("find the dirty data: " + value);
                }
            }
        });

        // TODO 4.使用FlinkCDC读取MySQL配置信息表创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        // TODO 5.将配置流处理为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-status", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);

        // TODO 6.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterJsonDS.connect(broadcastStream);

        // TODO 7.处理连接流,根据配置信息处理主流数据
//        SingleOutputStreamOperator<Object> dimDS = connectedStream.process(new BroadcastProcessFunction<JSONObject, String, JSONObject>() {
//        });
        SingleOutputStreamOperator<JSONObject> dimDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor));

        // TODO 8.将数据写出到Phoenix
        dimDS.print("######### dimDS ##############");
        dimDS.addSink(new DimSinkFunction());

        // TODO 9.启动任务
        env.execute("DimApp");
    }
}
