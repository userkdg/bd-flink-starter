package cn.com.bd.flinksql.upsert.kafka;

import cn.com.bd.flinksql.common.UpSertKafkaSQLConstants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 刘天能
 * @createAt 2021-01-28 16:17
 * @description
 */
public class ConsumerUpsertKafka {
    public static void main(String[] args) {
        // 初始化FLink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        // 注册upsert Kafka
        tableEnv.executeSql(UpSertKafkaSQLConstants.REGISTER_UPSERT_KAFKA);

        // 分组聚合写入HBase
        tableEnv.executeSql(UpSertKafkaSQLConstants.HBASE_SCHEMA);
        tableEnv.executeSql(UpSertKafkaSQLConstants.INSERT_HBASE);
    }
}
