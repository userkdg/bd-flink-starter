package cn.com.bd.sql;

public class SQLConstants {

    public static final String SOURCE_TABLE ="rt_o_ec_test_test_order";
    public static final String SINK_TABLE = "rt_s_test_order";

    public static final String SQL_CREATE_TABLE = "create table " + SOURCE_TABLE + "("
            + " order_code int,"
            + " shop_code string,"
            + " order_pay_amt float,"
            + " order_status int,"
            + " create_time string"
            + " ) with ("
            + " 'connector' = 'kafka',"
            + " 'topic' = 'rt.test.test_order',"
            + " 'properties.bootstrap.servers' = '192.168.235.51:9092,192.168.235.52:9092,192.168.235.53:9092',"
            + " 'properties.group.id' = 'bd-flink-test',"
            + " 'format' = 'canal-json'"
            + " )";

    public static final String SQL_CREATE_TABLE_SINK = "create table " + SINK_TABLE + "("
            + " rowkey string, "
            + " info ROW<shop_code string, order_pay_amt string>,"
            + " PRIMARY KEY (rowkey) NOT ENFORCED"
            + " ) with ("
            + " 'connector' = 'hbase-1.4',"
            + " 'table-name' = 'rt_s_test_order',"
            + " 'sink.buffer-flush.max-rows'='1',"
            + " 'sink.buffer-flush.interval'='1s',"
            + " 'zookeeper.quorum' = '192.168.235.51:2181,192.168.235.52:2181,192.168.235.53:2181'"
            + ")";

    public static final String SQL_QUERY = "create view rt_s_order_shop_sum "
            + " as select shop_code, cast(sum(order_pay_amt) as varchar) as order_pay_amt from " + SQLConstants.SOURCE_TABLE + " where order_status >= 3 group by shop_code";

    public static final String SQL_R_1 = "insert into rt_s_test_order "
            + " select shop_code as rowkey, ROW(shop_code, order_pay_amt) from rt_s_order_shop_sum";
}
