package cn.com.bd.flinksql.common;

/**
 * @author 刘天能
 * @createAt 2021-01-28 15:37
 * @description FLink sql upSert kafka SQL
 */
public class UpSertKafkaSQLConstants {
    // 注册order_status表
    public final static String ODS_EC_ORDER_STATUS = "create table ec_order_status (\n" +
            " order_code string,\n" +
            " order_type bigint,\n" +
            " order_status bigint,\n" +
            " handle_type bigint," +
            " pt as proctime()\n" +
            ") with (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'flink_test',\n" +
            " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092',\n" +
            " 'properties.group.id' = 'testGroup',\n" +
            " 'scan.startup.mode' = 'earliest-offset',\n" +
            " 'format' = 'json'\n" +
            ")";

    // 注册order表
    public final static String ODS_EC_ORDER = "create table ec_order (\n" +
            " order_code string,\n" +
            " pay_amt decimal(12,2)\n" +
            ") with (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'flink_order_test',\n" +
            " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092',\n" +
            " 'properties.group.id' = 'testGroup',\n" +
            " 'scan.startup.mode' = 'earliest-offset',\n" +
            " 'format' = 'json'\n" +
            ")";

    // order表与order_status表join生成临时表, 因为常规双流join会产生一对多的情形，所以根据proc_time取top1进行join
    public final static String ORDER_JOIN_RES = "create view join_res as" +
            " select s.order_code, s.order_type, s.order_status, s.handle_type, o.pay_amt" +
            " from ec_order o inner join (select * from (select *," +
            " row_number() over (partition by order_code order by pt desc) as row_num" +
            " from ec_order_status" +
            " )" +
            " where row_num = 1" +
            ") s" +
            " on s.order_code = o.order_code";

    public final static String REGISTER_UPSERT_KAFKA = "create table kafka_upsert (\n" +
            " order_code string,\n" +
            " order_type bigint,\n" +
            " order_status bigint,\n" +
            " handle_type bigint,\n" +
            " pay_amt decimal(12, 2)," +
            " primary key(order_code) not enforced" +
            ") with (\n" +
            " 'connector' = 'upsert-kafka',\n" +
            " 'topic' = 'kafka_upsert_up',\n" +
            " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092',\n" +
            "  'key.format' = 'json',\n" +
            "  'key.json.ignore-parse-errors' = 'true'," +
            "  'value.json.fail-on-missing-field' = 'false',\n" +
            "  'value.format' = 'json'" +
            ")";

    // 定义HBase schema
    public final static String HBASE_SCHEMA = "create table hBaseTable (\n" +
            "        rowkey STRING,\n" +
            "        info ROW<pay_amt STRING, order_type STRING>,\n" +
            "        PRIMARY KEY (rowkey) NOT ENFORCED\n" +
            "        ) with (\n" +
            "        'connector' = 'hbase-1.4',\n" +
            "        'sink.buffer-flush.max-rows'='1',\n" +
            "        'sink.buffer-flush.interval'='1s',\n" +
            "        'table-name' = 'upsert_kafka_test',\n" +
            "        'zookeeper.quorum' = '192.168.243.25:2181,192.168.243.26:2181,192.168.243.27:2181'\n" +
            "       )";

    public final static String INSERT_HBASE = "insert into hBaseTable\n" +
            " select order_type as rowkey, ROW(pay_amt, order_type)\n" +
            " from (\n" +
            " select  cast(sum(pay_amt) as varchar) as pay_amt, cast(t.order_type as varchar) as order_type\n" +
            " from kafka_upsert t" +
            " group by t.order_type\n" +
            ")";
}
