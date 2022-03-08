package cn.com.bd.flinksql.common;

/**
 * @author admin
 * @createAt 2021-01-28 15:37
 * @description FLink sql upSert kafka SQL
 */
public class UpSertKafkaSQLConstants {
    // 注册order_status表
    public final static String ODS_EC_ORDER_STATUS = "create table ec_order_status (" +
            " order_code string," +
            " order_type bigint," +
            " order_status bigint," +
            " handle_type bigint," +
            " pt as proctime()" +
            " ) with (" +
            " 'connector' = 'kafka'," +
            " 'topic' = 'flink_common_json_order_status'," +
            " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092'," +
            " 'properties.group.id' = 'testGroup'," +
            " 'scan.startup.mode' = 'latest-offset'," +
            " 'format' = 'json'" +
            " )";

    // 注册order表
    public final static String ODS_EC_ORDER = "create table ec_order (" +
            " order_code string," +
            " pay_amt decimal(12,2)," +
            " pt as proctime()" +
            " ) with (" +
            " 'connector' = 'kafka'," +
            " 'topic' = 'flink_common_json_order'," +
            " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092'," +
            " 'properties.group.id' = 'testGroup'," +
            " 'scan.startup.mode' = 'latest-offset'," +
            " 'format' = 'json'" +
            " )";

    // order表与order_status表join生成临时表, 因为常规双流join会产生一对多的情形，所以根据proc_time取top1进行join
    public final static String ORDER_JOIN_RES = "create view join_res as" +
            " select s.order_code, s.order_type, s.order_status, s.handle_type, o.pay_amt" +
            " from (select * from (select *," +
            " row_number() over (partition by order_code order by pt desc) as row_num" +
            " from ec_order" +
            " ) " +
            " where row_num = 1" +
            " ) o inner join (select * from (select *," +
            " row_number() over (partition by order_code order by pt desc) as row_num" +
            " from ec_order_status" +
            " )" +
            " where row_num = 1" +
            " ) s" +
            " on s.order_code = o.order_code";

    public final static String REGISTER_UPSERT_KAFKA = "create table kafka_upsert (" +
            " order_code string," +
            " order_type bigint," +
            " order_status bigint," +
            " handle_type bigint," +
            " pay_amt decimal(12, 2)," +
            " primary key(order_code) not enforced" +
            " ) with (" +
            " 'connector' = 'upsert-kafka'," +
            " 'topic' = 'kafka_upsert_order_res'," +
            " 'properties.bootstrap.servers' = '192.168.243.38:9092,192.168.243.39:9092,192.168.243.40:9092'," +
            " 'key.format' = 'json'," +
            " 'key.json.ignore-parse-errors' = 'true'," +
            " 'value.json.fail-on-missing-field' = 'false'," +
            " 'value.format' = 'json'" +
            " )";

    public final static String REGISTER_KAFKA = "create table kafka_table (" +
            " order_code string," +
            " order_type bigint," +
            " order_status bigint," +
            " handle_type bigint," +
            " pt timestamp" +
            " ) with (" +
            " 'connector' = 'kafka'," +
            " 'topic' = 'kafka_order_res'," +
            " 'properties.bootstrap.servers' = '192.168.243.38:9092,192.168.243.39:9092,192.168.243.40:9092'," +
            " 'value.format' = 'json'" +
            " )";

    // 定义HBase schema
    public final static String HBASE_SCHEMA = "create table hBaseTable (" +
            "        rowkey STRING," +
            "        info ROW<pay_amt STRING, order_type STRING>," +
            "        PRIMARY KEY (rowkey) NOT ENFORCED" +
            "        ) with (" +
            "        'connector' = 'hbase-1.4'," +
            "        'sink.buffer-flush.max-rows'='1'," +
            "        'sink.buffer-flush.interval'='1s'," +
            "        'table-name' = 'upsert_kafka_test'," +
            "        'zookeeper.quorum' = '192.168.243.25:2181,192.168.243.26:2181,192.168.243.27:2181'" +
            "       )";

    public final static String INSERT_HBASE = "insert into hBaseTable" +
            " select order_type as rowkey, ROW(pay_amt, order_type)" +
            " from (" +
            " select  cast(sum(pay_amt) as varchar) as pay_amt, cast(t.order_type as varchar) as order_type" +
            " from kafka_upsert t" +
            " group by t.order_type" +
            " )";
}
