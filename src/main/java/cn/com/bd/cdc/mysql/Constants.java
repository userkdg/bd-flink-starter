package cn.com.bd.cdc.mysql;

/**
 * @author 刘天能
 * @createdAt 2021-04-06 15:11
 * @description
 */
public class Constants {
    // 定义Flink SQL CDC table
    public final static String REGISTER_CDC_TABLE = "create table express (\n" +
            " data_id string," +
            " pk_express_id bigint," +
            " express_code bigint," +
            " express_name string," +
            " start_time string," +
            " end_time string," +
            " etl_datetime timestamp" +
            ") with (" +
            "'connector' = 'mysql-cdc'," +
            "'scan.startup.mode' = 'initial'," +
            "'hostname' = '192.168.234.7'," +
            "'port' = '3306'," +
            "'username' = 'canal'," +
            "'password' = 'canal'," +
            "'server-time-zone' = 'Asia/Shanghai'," +
            "'database-name' = 'ec_order'," +
            "'table-name' = 'dim_ec_ldp_express_test'" +
            ")";

    // 定义changelog Kafka
    public final static String DEBEZIUM_KAFKA = "create table debezium_kafka (" +
            " data_id string," +
            " pk_express_id bigint," +
            " express_code bigint," +
            " express_name string," +
            " start_time string," +
            " end_time string," +
            " etl_datetime timestamp" +
            " ) with (" +
            " 'connector' = 'kafka'," +
            " 'topic' = 'mysql_cdc_test'," +
            " 'properties.bootstrap.servers' = '192.168.235.51:9092,192.168.235.52:9092,192.168.235.53:9092'," +
            " 'properties.group.id' = 'testGroup'," +
            " 'scan.startup.mode' = 'earliest-offset'," +
            " 'format' = 'debezium-json'" +
            ")" +
            "";
}
