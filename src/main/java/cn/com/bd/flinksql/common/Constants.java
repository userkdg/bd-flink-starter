package cn.com.bd.flinksql.common;

/**
 * @author 刘天能
 * @createAt 2021-01-19 15:03
 * @description 常量类
 */
public class Constants {
    // 将rt.test.ec_oms_order_status_2 topic注册成FLink table：ec_order_status
    public final static String EC_ORDER_STATUS_KAFKA_TABLE_SQL = "create table ec_order_status (\n" +
            "order_code string,\n" +
            "order_type bigint,\n" +
            "order_status bigint,\n" +
            "handle_type bigint\n" +
            ") with (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'rt.test.ec_oms_order_status_2',\n" +
            " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092',\n" +
            " 'properties.group.id' = 'testGroup',\n" +
            " 'scan.startup.mode' = 'earliest-offset',\n" +
            " 'format' = 'canal-json'\n" +
            ")";

    // 将rt.test.ec_oms_order_2 topic注册成FLink table：ec_order
    public final static String EC_ORDER_KAFKA_TABLE_SQL = "create table ec_order (\n" +
            "order_code string,\n" +
            "shop_code string,\n" +
            "shop_order_id string,\n" +
            "storehouse_name string,\n" +
            "order_pay_amt decimal(20, 2)\n" +
            ") with (\n" +
            "'connector' = 'kafka',\n" +
            " 'topic' = 'rt.test.ec_oms_order_2',\n" +
            " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092',\n" +
            " 'properties.group.id' = 'testGroup',\n" +
            " 'scan.startup.mode' = 'earliest-offset',\n" +
            " 'format' = 'canal-json'\n" +
            ")";

    // 注册HBase catalog
    public final static String HBASE_CATALOG_SQL = "create table hBaseTable (\n" +
            "        rowkey STRING,\n" +
            "        info ROW<order_pay_amt decimal(38, 2), warehouse_name STRING>,\n" +
            "        PRIMARY KEY (rowkey) NOT ENFORCED\n" +
            "        ) with (\n" +
            "        'connector' = 'hbase-1.4',\n" +
            "        'sink.buffer-flush.max-rows'='1',\n" +
            "        'sink.buffer-flush.interval'='1s',\n" +
            "        'table-name' = 'ec_order_join_test',\n" +
            "        'zookeeper.quorum' = '192.168.243.25:2181,192.168.243.26:2181,192.168.243.27:2181'\n" +
            "       )";

    // 实现双流join，求出指标后将数据写入HBase
    public final static String INSET_HBASE_SQL = "insert into hBaseTable\n" +
            "select warehouse_name as rowkey, ROW(order_pay_amt, warehouse_name)\n" +
            "from (\n" +
            "select  sum(order_pay_amt) as order_pay_amt, t.storehouse_name as warehouse_name\n" +
            "from (\n" +
            "select o.order_code, s.order_status, o.storehouse_name, o.order_pay_amt\n" +
            "from ec_order o inner join ec_order_status s\n" +
            "on o.order_code = s.order_code\n" +
            ") t group by t.storehouse_name\n" +
            ")";
}
