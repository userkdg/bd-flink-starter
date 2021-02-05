package cn.com.bd.sql;

import cn.com.bd.func.map.OrderMap;
import cn.com.bd.pojo.Order;
import cn.com.bd.utils.FieldUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * 目的：
 * 如果在处理过程中，表是通过数据流生成的，或者，将表转成数据流并通过一些操作进行处理后，再转换成表，不管中间经过多少次
 * 表及数据流之间的转换，那么，当sink端统一都是通过sql的insert into到外部存储时，程序都不需要再执行env.execute()来触
 * 发作业的调度执行
 *
 *
 * 1.在socket服务器上执行命令（分别开两个shell窗口）：nc -l 9990
 * 2.数据：（9990上输入）
 * 1,10,京东
 * 2,20,天猫
 * 3,30,京东
 * 4,40,京东
 * 4,50,天猫
 *
 *
 * hbase建表语句：
 * create 'rt_d_test_order','info'
 *
 * phoenix建表语句：
 * create table "rt_d_test_order" (
 * "id" varchar primary key,
 * "info"."order_code" varchar,
 * "info"."payment" varchar,
 * "info"."platform" varchar
 * )
 *
 */
public class SQLTest2 {
    private final static String ORDER_TABLE = "ec_order";
    private final static String ORDER_FIELDS = FieldUtils.getFieldNames(Order.class);
    public static final String SINK_TABLE = "rt_d_test_order";
    public static final String SQL_CREATE_TABLE_SINK = "create table " + SINK_TABLE + "("
            + " rowkey string, "
            + " info ROW<order_code string, payment string, platform string>,"
            + " PRIMARY KEY (rowkey) NOT ENFORCED"
            + " ) with ("
            + " 'connector' = 'hbase-1.4',"
            + " 'table-name' = 'rt_d_test_order',"
            + " 'sink.buffer-flush.max-rows'='1',"
            + " 'sink.buffer-flush.interval'='1s',"
            + " 'zookeeper.quorum' = '192.168.235.51:2181,192.168.235.52:2181,192.168.235.53:2181'"
            + ")";
    private static final String SQL = "insert into " + SINK_TABLE
            + " select order_code as rowkey, ROW(order_code, payment, platform) from "
            + " (select cast(orderCode as varchar) as order_code, cast(payment as varchar) as payment, platform from " + ORDER_TABLE + ") temp_order";

    public static void main(String[] args) throws Exception {
        String serverIp = "192.168.235.12";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        DataStream<Order> orderStream = env.socketTextStream(serverIp, 9990, "\n").map(new OrderMap());
        tableEnv.createTemporaryView(ORDER_TABLE, orderStream.filter(order -> order.getPlatform().equals("京东")), FieldUtils.convertFieldArray(ORDER_FIELDS));
        tableEnv.executeSql(SQL_CREATE_TABLE_SINK);
        tableEnv.executeSql(SQL);
    }
}
