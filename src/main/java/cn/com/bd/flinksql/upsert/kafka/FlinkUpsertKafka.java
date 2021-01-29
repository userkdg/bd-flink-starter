package cn.com.bd.flinksql.upsert.kafka;

import cn.com.bd.flinksql.common.UpSertKafkaSQLConstants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 刘天能
 * @createAt 2021-01-25 9:20
 * @description FlinkSQL 主键测试
 */
public class FlinkUpsertKafka {
    public static void main(String[] args) {
        // 初始化FLink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        // 将Kafka注册成FlLink table
        tableEnv.executeSql(UpSertKafkaSQLConstants.ODS_EC_ORDER_STATUS);
        tableEnv.executeSql(UpSertKafkaSQLConstants.ODS_EC_ORDER);

        // 注册upsert-kafka
        tableEnv.executeSql(UpSertKafkaSQLConstants.REGISTER_UPSERT_KAFKA);

        // 实现双流join
        tableEnv.executeSql(UpSertKafkaSQLConstants.ORDER_JOIN_RES);

        // 将数据以upsert-kafka方式写入Kafka
        tableEnv.executeSql("insert into kafka_upsert select * from join_res");
    }
}
