package cn.com.bd.table;

import cn.com.bd.func.filter.TwoJoinFilterByTimestamp;
import cn.com.bd.func.map.OrderMap;
import cn.com.bd.func.map.OrderStatusMap;
import cn.com.bd.pojo.Order;
import cn.com.bd.pojo.OrderDetail;
import cn.com.bd.pojo.OrderStatus;
import cn.com.bd.utils.FieldUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.types.Row;

/**
 * 1.在socket服务器上执行命令（分别开两个shell窗口）：
 * nc -l 9990
 * nc -l 9991
 *
 * 2.数据：
 * orders:（9990上输入）
 * 1,10,京东
 * 1,20,京东
 * 1,30,京东
 * 1,40,京东
 * 1,50,京东
 * 1,60,京东
 *
 * orderStatus:（9991上输入）
 * 1,2020-03-28 09:10:00,1
 * 1,2020-03-28 09:20:00,2
 * 1,2020-03-28 09:31:00,3
 * 1,2020-03-28 09:40:00,4
 */
public class TableJoin {
    private final static String ORDER_TABLE = "ec_order";
    private final static String ORDER_STATUS_TABLE = "order_status";
    private final static String ORDER_JOIN = "order_join";
    private final static String ORDER_FIELDS = FieldUtils.getFieldNames(Order.class);
    private final static String ORDER_STATUS_FIELDS = FieldUtils.getFieldNames(OrderStatus.class);
    private final static String JOIN_SQL = "select o.*,os.payTime,os.handleType,os.timestamp2 from " + ORDER_TABLE + " o inner join " + ORDER_STATUS_TABLE + " os on o.orderCode=os.orderCode";
    private final static String SQL2 = "select platform, sum(payment) as payment from (select * from (" +
            "select orderCode, payment, payTime, handleType, platform, row_number() over(partition by orderCode order by payTime desc) as row_num " +
            "from " + ORDER_JOIN + ") t " +
            "where t.row_num=1) t2 group by platform";

    public static void main(String[] args) throws Exception {
        String serverIp = "192.168.243.21";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        DataStream<Order> orderStream = env.socketTextStream(serverIp, 9990, "\n").map(new OrderMap());
        DataStream<OrderStatus> orderStatusStream = env.socketTextStream(serverIp, 9991, "\n").map(new OrderStatusMap());
        tableEnv.createTemporaryView(ORDER_TABLE, orderStream, FieldUtils.convertFieldArray(ORDER_FIELDS));
        tableEnv.createTemporaryView(ORDER_STATUS_TABLE, orderStatusStream, FieldUtils.convertFieldArray(ORDER_STATUS_FIELDS));
        Table table = tableEnv.sqlQuery(JOIN_SQL);
        DataStream<OrderDetail> ddStream = tableEnv.toRetractStream(table, OrderDetail.class)
                  .map(i -> i.f1)
                  .keyBy(OrderDetail::getOrderCode)
                  .filter(new TwoJoinFilterByTimestamp("orderDetail"));
        tableEnv.createTemporaryView(ORDER_JOIN, ddStream, FieldUtils.convertFieldArray(FieldUtils.getFieldNames(OrderDetail.class)));
        table = tableEnv.sqlQuery(SQL2);
        tableEnv.toRetractStream(table, Row.class)
                .print();
        env.execute();
    }
}
