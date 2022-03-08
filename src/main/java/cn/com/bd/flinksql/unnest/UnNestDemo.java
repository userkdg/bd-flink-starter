package cn.com.bd.flinksql.unnest;

import cn.com.bd.flinksql.common.UnNestSQLConstants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.functions.SqlUnnestUtils;

/**
 * @author admin
 * @createdAt 2021-04-25 15:52
 * @description Flinksql 列转行示例
 */
public class UnNestDemo {
    public static void main(String[] args) {
        // 初始化FLink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        tableEnv.executeSql(UnNestSQLConstants.EC_ORDER_KAFKA_TABLE_SQL);

        tableEnv.executeSql("select o.*, t.k from ec_order o," +
                " UNNEST(STR_TO_MAP(cast(order_pay_amt as varchar), '\\.', '='))as t(k, v)").print();
    }
}
