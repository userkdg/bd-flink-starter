package cn.com.bd.flinksql;

import cn.com.bd.flinksql.common.Constants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
    实现整体思路：
        1. 将Kafka topic中的数据注册成FLink的表，此时跟我们平常认知的像mysql的表一样
            注册的形式采用的是canal-json，因为我们的数据是canal采集的mysql binlog数据写入到Kafka

        2. 注册HBase的catalog，为数据写入HBase做准备

        3. 实现FLink table的双流join，完成想要做的group by操作，求出需要的指标

        4. 将数据写入HBase
 */

/**
 * @author 刘天能
 * @createAt 2021-01-19 14:59
 * @description FLinkSQL 双流join Demo
 */
public class EcOrderJoinTest {
    public static void main(String[] args) {
        // 初始化FLink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        // 将Kafka注册成表
        tableEnv.executeSql(Constants.EC_ORDER_STATUS_KAFKA_TABLE_SQL);
        tableEnv.executeSql(Constants.EC_ORDER_KAFKA_TABLE_SQL);

        // 注册HBase catalog
        tableEnv.executeSql(Constants.HBASE_CATALOG_SQL);

        // 实现双流join，求出指标，然后将数据写入HBase
        tableEnv.executeSql(Constants.INSET_HBASE_SQL);
    }
}
