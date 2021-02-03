package cn.com.bd.flinksql.api;

import cn.com.bd.flinksql.common.NotExistsSQLConstants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * @author 刘天能
 * @createAt 2021-02-02 10:40
 * @description
 */
public class EcOrderSaleTest {
    public static void main(String[] args) throws Exception{
        // 初始化FLink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        // todo 注册订单表以及订单状态数据源表
        tableEnv.executeSql(NotExistsSQLConstants.STATUS_KAFKA_TABLE_SQL);
        tableEnv.executeSql(NotExistsSQLConstants.ORDER_KAFKA_TABLE_SQL);

        // todo 实现双流join，生成中间表
        tableEnv.executeSql(NotExistsSQLConstants.DOUBLE_STREAM_JOIN_SQL);

        // todo 生成订单事实表
        tableEnv.executeSql(NotExistsSQLConstants.ORDER_ACT);
        // todo 生成退款事实表
        tableEnv.executeSql(NotExistsSQLConstants.RFND_ORDER_ACT);
        // todo 生成有效订单事实表
        tableEnv.executeSql(NotExistsSQLConstants.EFF_ORDER_ACT);

        // todo 注册HBase schema
        tableEnv.executeSql(NotExistsSQLConstants.CUSTOM_CNT_HBASE_SCHEMA);
        tableEnv.executeSql(NotExistsSQLConstants.ORDER_HBASE_SCHEMA);
        tableEnv.executeSql(NotExistsSQLConstants.DISTINCT_CUS_CNT_HBASE_SCHEMA);
        tableEnv.executeSql(NotExistsSQLConstants.EFF_ORDER_HBASE_SCHEMA);

        // todo 求出聚合值
        tableEnv.executeSql(NotExistsSQLConstants.ORDER_GROUP_BY_SQL);
        tableEnv.executeSql(NotExistsSQLConstants.RFND_ORDER_GROUP_BY_SQL);
        tableEnv.executeSql(NotExistsSQLConstants.CUS_CNT_SQL);
        tableEnv.executeSql(NotExistsSQLConstants.EXISTS_CUS);

        Table table = tableEnv.sqlQuery("select * from cus_cnt");
        tableEnv.toRetractStream(table, Row.class).print();

        // todo 基于明细宽表进行指标计算，写入HBase
        tableEnv.executeSql(NotExistsSQLConstants.INSERT_INTO_HBASE_CUS_CNT);
        tableEnv.executeSql(NotExistsSQLConstants.INSERT_INTO_HBASE_ORDER);
        tableEnv.executeSql(NotExistsSQLConstants.INSERT_INTO_HBASE_EFF);
        tableEnv.executeSql(NotExistsSQLConstants.INSERT_INTO_REAL_CUS);

        env.execute();
    }
}
