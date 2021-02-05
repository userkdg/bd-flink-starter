package cn.com.bd.flinksql.upsert.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author 刘天能
 * @createAt 2021-02-04 15:41
 * @description
 */
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        tableEnv.executeSql("create table kafka_upsert (" +
                " name string," +
                " age bigint," +
                " gender string," +
                " age bigint," +
                " id bigint," +
                " primary key(name) not enforced" +
                " ) with (" +
                " 'connector' = 'upsert-kafka'," +
                " 'topic' = 'test_lyl3'," +
                " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092'," +
                " 'key.format' = 'json'," +
                " 'key.json.ignore-parse-errors' = 'true'," +
                " 'value.json.fail-on-missing-field' = 'false'," +
                " 'value.format' = 'json'" +
                " )");

        Table table = tableEnv.sqlQuery("select * from kafka_upsert");

        tableEnv.toRetractStream(table, Row.class).print();

        env.execute();


    }
}
