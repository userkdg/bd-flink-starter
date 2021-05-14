package cn.com.bd.flinksql.common;

/**
 * @author 刘天能
 * @createdAt 2021-04-26 10:29
 * @description
 */
public class TopNSQLConstants {
    public final static String EC_ORDER_KAFKA_TABLE_SQL = "create table ec_order (" +
            " order_code string," +
            " shop_code string," +
            " shop_order_id string," +
            " storehouse_name string," +
            " order_pay_amt decimal(20, 2)," +
            " primary key(order_code) NOT ENFORCED" +
            ") with (" +
            " 'connector' = 'kafka'," +
            " 'topic' = 'rt.ec_order.ec_oms_order'," +
            " 'properties.bootstrap.servers' = '192.168.235.51:9092,192.168.235.52:9092,192.168.235.53:9092'," +
            " 'properties.group.id' = 'testGroup'," +
            " 'scan.startup.mode' = 'earliest-offset'," +
            " 'format' = 'canal-json'" +
            " )";

    public final static String STATUS_KAFKA_TABLE_SQL = "create table status (\n" +
            " order_code string,\n" +
            " order_type string,\n" +
            " order_status string,\n" +
            " pay_time timestamp," +
            " primary key(order_code) NOT ENFORCED\n" +
            ") with (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'rt.ec_order.ec_oms_order_status',\n" +
            " 'properties.bootstrap.servers' = '192.168.235.51:9092,192.168.235.52:9092,192.168.235.53:9092',\n" +
            " 'properties.group.id' = 'testGroup',\n" +
            " 'scan.startup.mode' = 'earliest-offset',\n" +
            " 'format' = 'canal-json'\n" +
            ")";

    public final static String RT_TMP_EC_ORDER = "create view rt_tmp_ec_order as select " +
            " o.order_code," +
            " o.order_pay_amt," +
            " o.shop_code," +
            " s.pay_time" +
            " from ec_order o" +
            " inner join " +
            " status s" +
            " on o.order_code = s.order_code" +
            " where substring(cast(s.pay_time as varchar), 0, 10) = '2021-04-26'";

    public final static String TOP_N_SQL = "create view top_n as select " +
            " shop_code," +
            " order_pay_amt" +
            " from (" +
            " select" +
            "   *," +
            "   ROW_NUMBER () OVER (PARTITION BY shop_code ORDER BY order_pay_amt DESC) as rownum " +
            "   from (" +
            "       select " +
            "           shop_code," +
            "           order_pay_amt" +
            "       from rt_tmp_ec_order" +
            "       where " +
            "           order_pay_amt > 0" +
            ") a" +
            " ) " +
            " where rownum <= 2" +
            "  ";

    public final static String hbASE_SCHEMA = "create table hTable (" +
            "                                    rowkey STRING," +
            "                                    info ROW<order_code STRING, order_pay_amt STRING>," +
            "                                    PRIMARY KEY (rowkey) NOT ENFORCED" +
            "                                    ) with (" +
            "                                    'connector' = 'hbase-1.4'," +
            "                                    'sink.buffer-flush.max-rows'='1'," +
            "                                    'sink.buffer-flush.interval'='1s'," +
            "                                    'table-name' = 'top_n'," +
            "                                    'zookeeper.quorum' = '192.168.235.51:2181,192.168.235.52:2181,192.168.235.53:2181'" +
            "                                   )";

    public final static String INSERT_HBASE = "insert into hTable" +
            " select order_code as rowkey, row(shop_code, order_pay_amt) " +
            " from (" +
            "   select" +
            "     concat_ws('_', shop_code, cast(order_pay_amt as varchar)) as order_code," +
            "     shop_code," +
            "     cast(order_pay_amt as varchar) as order_pay_amt" +
            "   from top_n " +
            " )";


}
