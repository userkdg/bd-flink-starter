package cn.com.bd.flinksql.common;

/**
 * @author 刘天能
 * @createAt 2021-01-19 15:03
 * @description 常量类
 */
public class Constants {
    // 将rt.test.ec_oms_order_status_2 topic注册成FLink table：ec_order_status
    public final static String EC_ORDER_STATUS_KAFKA_TABLE_SQL = "create table ec_order_status (" +
            " order_code string," +
            " order_type bigint," +
            " order_status bigint," +
            " handle_type bigint" +
            " ) with (" +
            " 'connector' = 'kafka'," +
            " 'topic' = 'rt.test.ec_oms_order_status_2'," +
            " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092'," +
            " 'properties.group.id' = 'testGroup'," +
            " 'scan.startup.mode' = 'earliest-offset'," +
            " 'format' = 'canal-json'" +
            ")";

    // 将rt.test.ec_oms_order_2 topic注册成FLink table：ec_order
    public final static String EC_ORDER_KAFKA_TABLE_SQL = "create table ec_order (" +
            " order_code string," +
            " shop_code string," +
            " shop_order_id string," +
            " storehouse_name string," +
            " order_pay_amt decimal(20, 2)" +
            ") with (" +
            " 'connector' = 'kafka'," +
            " 'topic' = 'rt.test.ec_oms_order_2'," +
            " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092'," +
            " 'properties.group.id' = 'testGroup'," +
            " 'scan.startup.mode' = 'earliest-offset'," +
            " 'format' = 'canal-json'" +
            " )";

    // 注册HBase catalog
    public final static String HBASE_CATALOG_SQL = "create table hBaseTable (" +
            "        rowkey STRING," +
            "        info ROW<order_pay_amt decimal(38, 2), warehouse_name STRING>," +
            "        PRIMARY KEY (rowkey) NOT ENFORCED" +
            "        ) with (" +
            "        'connector' = 'hbase-1.4'," +
            "        'sink.buffer-flush.max-rows'='1'," +
            "        'sink.buffer-flush.interval'='1s'," +
            "        'table-name' = 'ec_order_join_test'," +
            "        'zookeeper.quorum' = '192.168.243.25:2181,192.168.243.26:2181,192.168.243.27:2181'" +
            "       )";

    // 实现双流join，求出指标后将数据写入HBase
    public final static String INSET_HBASE_SQL = "insert into hBaseTable" +
            " select warehouse_name as rowkey, ROW(order_pay_amt, warehouse_name)" +
            " from (" +
            " select  sum(order_pay_amt) as order_pay_amt, t.storehouse_name as warehouse_name" +
            " from (" +
            " select o.order_code, s.order_status, o.storehouse_name, o.order_pay_amt" +
            " from ec_order o inner join ec_order_status s" +
            " on o.order_code = s.order_code" +
            " ) t group by t.storehouse_name" +
            " )";
}
