package cn.com.bd.cdc.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 刘天能
 * @createdAt 2021-04-06 15:06
 * @description Flink mysql CDC Demo
 */
public class MysqlCDCDemo {
    public static void main(String[] args) {
        // 构建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
        env.enableCheckpointing(60000);

        // 创建MySQL CDC表
        tableEnv.executeSql(Constants.REGISTER_CDC_TABLE);

        // 注册changelog Kafka
        tableEnv.executeSql(Constants.CHANGELOG_KAFKA);

        tableEnv.executeSql(" insert into changelog_kafka select * from express");
    }
}
