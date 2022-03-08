package cn.com.bd.flinksql.upsert.kafka;

import cn.com.bd.flinksql.common.UpSertKafkaSQLConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.http.client.RedirectStrategy;

/**
 * @author admin
 * @createAt 2021-02-07 13:55
 * @description
 */
public class KafkaTwo {
    public static void main(String[] args) {
        // 初始化FLink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
        env.enableCheckpointing(600000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(10)));

        // 将Kafka注册成FlLink table
        tableEnv.executeSql(UpSertKafkaSQLConstants.ODS_EC_ORDER_STATUS);
        tableEnv.executeSql(UpSertKafkaSQLConstants.ODS_EC_ORDER);

        // 注册upsert-kafka
        tableEnv.executeSql(UpSertKafkaSQLConstants.REGISTER_KAFKA);

        // 实现双流join
        tableEnv.executeSql(UpSertKafkaSQLConstants.ORDER_JOIN_RES);

        tableEnv.executeSql("insert into kafka_table select * from ec_order_status");
    }
}
