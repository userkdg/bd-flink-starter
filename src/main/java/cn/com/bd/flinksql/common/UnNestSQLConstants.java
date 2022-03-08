package cn.com.bd.flinksql.common;

/**
 * @author admin
 * @createdAt 2021-04-25 15:54
 * @description
 */
public class UnNestSQLConstants {
    public final static String EC_ORDER_KAFKA_TABLE_SQL = "create table ec_order (" +
            " order_code string," +
            " shop_code string," +
            " shop_order_id string," +
            " storehouse_name string," +
            " order_pay_amt decimal(20, 2)" +
            ") with (" +
            " 'connector' = 'kafka'," +
            " 'topic' = 'rt.ec_order.ec_oms_order'," +
            " 'properties.bootstrap.servers' = '192.168.235.51:9092,192.168.235.52:9092,192.168.235.53:9092'," +
            " 'properties.group.id' = 'testGroup'," +
            " 'scan.startup.mode' = 'earliest-offset'," +
            " 'format' = 'canal-json'" +
            " )";
}
