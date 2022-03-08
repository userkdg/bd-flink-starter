package cn.com.bd.flinksql.upsert.kafka;

import cn.com.bd.flinksql.common.UpSertKafkaSQLConstants;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * @author admin
 * @createAt 2021-02-08 9:52
 * @description
 */
public class CommonKafka {
    public static void main(String[] args) throws Exception {
        // 初始化FLink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
        env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(30);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.minutes(1)));



//        // 将Kafka注册成FlLink table
        tableEnv.executeSql(UpSertKafkaSQLConstants.ODS_EC_ORDER_STATUS);

        tableEnv.executeSql("create view cal as select REGEXP_REPLACE('贵州省', '省', '') as res from ec_order_status");

        tableEnv.executeSql("create table printTable (" +
                "res string" +
                ") with (" +
                 " 'connector' = 'print'" +
                ")");

        tableEnv.executeSql("insert into printTable select * from cal");

        Table table = tableEnv.sqlQuery("select * from ec_order_status");
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        rowDataStream.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                Random random = new Random();
                int num = random.nextInt(5);
                if (num > 3) {
                    throw new RuntimeException("随机异常");
                }
                return null;
            }
        });

        tableEnv.executeSql("insert into ten_kafka_table select * from ec_order_status");

        env.execute();
    }
}
