package cn.com.bd.flinksql.common;

/**
 * @author 刘天能
 * @createAt 2021-02-02 10:41
 * @description 测试不存在函数的SQL
 */
public class NotExistsSQLConstants {
    // 将rt.test.ec_oms_order_status_test topic注册成FLink table：status
    public final static String STATUS_KAFKA_TABLE_SQL = "create table status (\n" +
            "order_code string,\n" +
            "order_type string,\n" +
            "order_status string,\n" +
            "pay_time timestamp\n" +
            ") with (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'rt.test.ec_oms_order_status_test',\n" +
            " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092',\n" +
            " 'properties.group.id' = 'testGroup',\n" +
            " 'scan.startup.mode' = 'earliest-offset',\n" +
            " 'format' = 'canal-json'\n" +
            ")";

    // 将rt.test.ec_oms_order_test topic注册成FLink table：order
    public final static String ORDER_KAFKA_TABLE_SQL = "create table ec_order (\n" +
            "order_code string,\n" +
            "buyer_nick string,\n" +
            "province string,\n" +
            "city string,\n" +
            "order_pay_amt decimal(20, 2)," +
            "order_received_amt decimal(20, 2)\n" +
            ") with (\n" +
            "'connector' = 'kafka',\n" +
            " 'topic' = 'rt.test.ec_oms_order_test',\n" +
            " 'properties.bootstrap.servers' = '192.168.243.25:9092,192.168.243.26:9092,192.168.243.27:9092',\n" +
            " 'properties.group.id' = 'testGroup',\n" +
            " 'scan.startup.mode' = 'earliest-offset',\n" +
            " 'format' = 'canal-json'\n" +
            ")";

    // 双流join
    public final static String DOUBLE_STREAM_JOIN_SQL = "create view order_view as select o.order_code, o.buyer_nick," +
            " o.province, o.city, order_pay_amt," +
            " o.order_received_amt, s.order_status, s.order_type, s.pay_time" +
            " from ec_order o inner join status s" +
            " on o.order_code = s.order_code";

    // 创建订单事实表
    public final static String ORDER_ACT = "create view order_act as select *" +
            " from order_view" +
            " where order_type in('1', '3') and order_status not in('1', '2', '3')";

    // 创建退款事实表
    public final static String RFND_ORDER_ACT = "create view rfnd_order_act as select *" +
            " from order_view" +
            " where order_type in('1', '3') and order_status = '4'";

    // 根据订单事实表进行聚合操作
    public final static String ORDER_GROUP_BY_SQL = "create view order_sum as select province, city," +
            " sum(order_received_amt) as order_received_amt," +
            " sum(order_pay_amt) as order_pay_amt" +
            " from order_act " +
            " group by province, city";

    // 根据退款事实表进行聚合操作
    public final static String RFND_ORDER_GROUP_BY_SQL = "create view rfnd_order_sum as select province, city," +
            " sum(order_received_amt) as rfnd_actual_amt," +
            " sum(order_pay_amt) as rfnd_pay_amt" +
            " from rfnd_order_act" +
            " group by province, city";

    // 根据订单事实表以及退款事实表求客户数以及退款客户数
    public final static String CUS_CNT_SQL = "create view cus_cnt as select od.province, od.city," +
            " count(distinct od.buyer_nick) as cntn_cus_qty," +
            " count(distinct case when rfnd_od.buyer_nick is not null then od.buyer_nick end) as rfnd_cus_qty," +
            " count(distinct case when rfnd_od.buyer_nick is null then od.buyer_nick end) as cus_qty" +
            " from order_act od " +
            " left join rfnd_order_act rfnd_od" +
            " on od.order_code = rfnd_od.order_code" +
            " group by od.province, od.city";

    public final static String EXISTS_CUS = "create view exists_cus as" +
            " select od.province, od.city, count(distinct buyer_nick) as cus_qty" +
            " from order_act od " +
            " where not exists (select order_code from rfnd_order_act r where od.order_code = r.order_code)" +
            " group by province, city";


    public final static String INSERT_INTO_REAL_CUS = "insert into di" +
            " select rowkey as rowkey, ROW(province, city, cus_qty) " +
            " from (" +
            " select concat_ws('_', province, city) as rowkey, province, city, cast(cus_qty as varchar) as cus_qty" +
            " from exists_cus" +
            " )";

    // 将数据写入HBase
    public final static String INSERT_INTO_HBASE_ORDER = "insert into sum_to_hbase" +
            " select rowkey as rowkey, ROW(province, city, order_received_amt, order_pay_amt)" +
            " from (" +
            " select concat_ws('_', province, city) as rowkey, cast(order_received_amt as varchar) as order_received_amt," +
            " cast(order_pay_amt as varchar) as order_pay_amt, province, city" +
            " from order_sum" +
            " )";
    public final static String INSERT_INTO_HBASE_RFND = "insert into sum_to_hbase" +
            " select rowkey as rowkey, ROW(province, city, rfnd_actual_amt, rfnd_pay_amt)" +
            " from (" +
            " select concat_ws('_', province, city) as rowkey, cast(rfnd_actual_amt as varchar) as rfnd_actual_amt," +
            " cast(rfnd_pay_amt as varchar) as rfnd_pay_amt, province, city" +
            " from rfnd_order_sum" +
            " )";
    public final static String INSERT_INTO_HBASE_CUS_CNT = "insert into cnt_to_hnbase" +
            " select rowkey as rowkey, ROW(cntn_cus_qty, rfnd_cus_qty, cus_qty, province, city) " +
            " from (" +
            " select concat_ws('_', province, city) as rowkey, province, city, cast(cntn_cus_qty as varchar) as cntn_cus_qty," +
            " cast(rfnd_cus_qty as varchar) as rfnd_cus_qty, cast(cus_qty as varchar) as cus_qty" +
            " from cus_cnt" +
            " )";

    // 创建订单汇总表的HBase schema
    public final static String ORDER_HBASE_SCHEMA = "create table sum_to_hbase (\n" +
            "        rowkey STRING,\n" +
            "        info ROW<order_pay_amt STRING, order_received_amt STRING, order_cnt STRING, rfnd_actual_amt STRING, rfnd_pay_amt STRING, rfnd_order_cnt STRING, province STRING, city STRING>,\n" +
            "        PRIMARY KEY (rowkey) NOT ENFORCED\n" +
            "        ) with (\n" +
            "        'connector' = 'hbase-1.4',\n" +
            "        'sink.buffer-flush.max-rows'='1',\n" +
            "        'sink.buffer-flush.interval'='1s',\n" +
            "        'table-name' = 'rt_s_ec_sa_area_order_sum',\n" +
            "        'zookeeper.quorum' = '192.168.235.51:2181,192.168.235.52:2181,192.168.235.53:2181'\n" +
            "       )";

    // 创建用户数的HBase schema
    public final static String CUSTOM_CNT_HBASE_SCHEMA = "create table cnt_to_hnbase (\n" +
            "        rowkey STRING,\n" +
            "        info ROW<cntn_cus_qty STRING, rfnd_cus_qty STRING, cus_qty STRING, province STRING, city STRING>,\n" +
            "        PRIMARY KEY (rowkey) NOT ENFORCED\n" +
            "        ) with (\n" +
            "        'connector' = 'hbase-1.4',\n" +
            "        'sink.buffer-flush.max-rows'='1',\n" +
            "        'sink.buffer-flush.interval'='1s',\n" +
            "        'table-name' = 'rt_s_ec_sa_area_cus_count',\n" +
            "        'zookeeper.quorum' = '192.168.235.51:2181,192.168.235.52:2181,192.168.235.53:2181'\n" +
            "       )";

    // 创建schema
    public final static String DISTINCT_CUS_CNT_HBASE_SCHEMA = "create table di (\n" +
            "        rowkey STRING,\n" +
            "        info ROW<cus_qty STRING, province STRING, city STRING>,\n" +
            "        PRIMARY KEY (rowkey) NOT ENFORCED\n" +
            "        ) with (\n" +
            "        'connector' = 'hbase-1.4',\n" +
            "        'sink.buffer-flush.max-rows'='1',\n" +
            "        'sink.buffer-flush.interval'='1s',\n" +
            "        'table-name' = 'rt_s_real_cus_count',\n" +
            "        'zookeeper.quorum' = '192.168.235.51:2181,192.168.235.52:2181,192.168.235.53:2181'\n" +
            "       )";
}