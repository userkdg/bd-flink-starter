package cn.com.bd.flinksql.upsert.kafka;

import cn.com.bd.flinksql.common.UpSertKafkaSQLConstants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 样例数据：
 * {"order_code":"1","shop_name":"天猫","order_status":3,"pay_amt":10}
 * {"order_code":"1","shop_name":"天猫","order_status":3,"pay_amt":20}
 * {"order_code":"1","shop_name":"天猫","order_status":4,"pay_amt":20}
 * {"order_code":"2","shop_name":"京东","order_status":3,"pay_amt":20}
 * {"order_code":"2","shop_name":"京东","order_status":3,"pay_amt":50}
 * {"order_code":"3","shop_name":"天猫","order_status":3,"pay_amt":20}
 * {"order_code":"3","shop_name":"天猫","order_status":3,"pay_amt":50}
 *
 * 生产数据：
 * kafka-console-producer --broker-list 192.168.235.51:9092,192.168.235.52:9092,192.168.235.53:9092 --topic rt.test_ec_order
 *
 * 消费upsert-kafka数据：
 * kafka-console-consumer --bootstrap-server 192.168.235.51:9092,192.168.235.52:9092,192.168.235.53:9092 --topic rt.test_d_ec_order
 *
 */
public class UpsertKafkaDemo {
    private static final String SOURCE_TABLE = "create table rt_o_ec_order (" +
            " order_code string," +
            " shop_name string," +
            " order_status bigint," +
            " pay_amt decimal(12, 2)," +
            " pt as proctime()" +
            " ) with (" +
            " 'connector' = 'kafka'," +
            " 'topic' = 'rt.test_ec_order'," +
            " 'properties.bootstrap.servers' = '192.168.235.51:9092,192.168.235.52:9092,192.168.235.53:9092'," +
            " 'properties.group.id' = 'UpsertKafkaDemo'," +
            " 'format' = 'json'" +
            " )";

    private static final String UPSERT_TABLE = "create table rt_d_ec_order (" +
            " order_code string," +
            " shop_name string," +
            " order_status bigint," +
            " pay_amt decimal(12, 2)," +
            " primary key(order_code) not enforced" +
            " ) with (" +
            " 'connector' = 'upsert-kafka'," +
            " 'topic' = 'rt.test_d_ec_order'," +
            " 'properties.bootstrap.servers' = '192.168.235.51:9092,192.168.235.52:9092,192.168.235.53:9092'," +
            " 'key.format' = 'json'," +
            " 'key.json.ignore-parse-errors' = 'true'," +
            " 'value.json.fail-on-missing-field' = 'false'," +
            " 'value.format' = 'json'" +
            " )";

    private static final String SINK_TABLE_1 = "create table rt_s_ec_sink_1 (" +
            " order_code string," +
            " shop_name string," +
            " order_status bigint," +
            " pay_amt decimal(12, 2)" +
            " ) with (" +
            " 'connector' = 'print'" +
            " )";

    private static final String SINK_TABLE_2 = "create table rt_s_ec_sink_2 (" +
            " shop_name string," +
            " pay_amt decimal(12, 2)" +
            " ) with (" +
            " 'connector' = 'print'" +
            " )";

    public static void main(String[] args) throws Exception {
        // 初始化FLink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        /**
         * 注册表
         */
        tableEnv.executeSql(SOURCE_TABLE);
        tableEnv.executeSql(UPSERT_TABLE);
        tableEnv.executeSql(SINK_TABLE_1);
        tableEnv.executeSql(SINK_TABLE_2);

        /**
         * 将普通json数据流以upsert-kafka的格式写到另一个topic中
         */
        tableEnv.executeSql("insert into rt_d_ec_order select order_code,shop_name,order_status,pay_amt from rt_o_ec_order ");

        tableEnv.executeSql("insert into rt_s_ec_sink_1 " +
                "select " +
                "  order_code," +
                "  shop_name, " +
                "  order_status, " +
                "  pay_amt " +
                "from rt_d_ec_order " +
                "where order_status = 3");

        tableEnv.executeSql("insert into rt_s_ec_sink_2 " +
                "select " +
                "  shop_name, " +
                "  sum(pay_amt) as pay_amt " +
                "from rt_d_ec_order " +
                "where order_status = 3" +
                "group by shop_name");
    }

}
