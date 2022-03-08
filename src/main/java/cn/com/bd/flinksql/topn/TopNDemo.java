package cn.com.bd.flinksql.topn;

import cn.com.bd.flinksql.common.TopNSQLConstants;
import cn.com.bd.flinksql.common.UnNestSQLConstants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author admin
 * @createdAt 2021-04-26 10:26
 * @description top n案例代码
 */
public class TopNDemo {
    public static void main(String[] args) {
        // 初始化FLink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);
        tableEnv.getConfig().getConfiguration().setString("table.exec.mini-batch.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("blink.microBatch.allowLatencyMs", "5000");
        tableEnv.getConfig().getConfiguration().setString("table.exec.mini-batch.allow-latency", "5000");
        tableEnv.getConfig().getConfiguration().setString("table.exec.mini-batch.size", "2000");
        tableEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "10 s");
        tableEnv.getConfig().getConfiguration().setBoolean("table.exec.source.cdc-events-duplicate", true);
        tableEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);
        tableEnv.getConfig().getConfiguration().setBoolean("blink.localAgg.enabled", true);

        // 源表注册
        tableEnv.executeSql(TopNSQLConstants.EC_ORDER_KAFKA_TABLE_SQL);
        tableEnv.executeSql(TopNSQLConstants.STATUS_KAFKA_TABLE_SQL);

        // join
        tableEnv.executeSql(TopNSQLConstants.RT_TMP_EC_ORDER);

        // 求出top 10
        tableEnv.executeSql(TopNSQLConstants.TOP_N_SQL);

//        tableEnv.executeSql("select * from top_n").print();

        tableEnv.executeSql(TopNSQLConstants.hbASE_SCHEMA);

        tableEnv.executeSql(TopNSQLConstants.INSERT_HBASE);

    }
}
