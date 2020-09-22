package cn.com.bd.table;

import cn.com.bd.func.map.OrderMap;
import cn.com.bd.func.map.OrderStatusMap;
import cn.com.bd.pojo.Order;
import cn.com.bd.pojo.OrderDetail;
import cn.com.bd.pojo.OrderStatus;
import cn.com.bluemoon.bd.flink.func.filter.TwoJoinFilterByTimestamp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

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
 * 1,2020-03-28 09:30:00,3
 * 1,2020-03-28 09:40:00,4
 */
public class TableJoin {
    private final static String ORDER_TABLE = "ec_order";
    private final static String ORDER_STATUS_TABLE = "order_status";
    private final static String ORDER_FIELDS = "orderCode,payment,platform,timestamp1";
    private final static String ORDER_STATUS_FIELDS = "orderCode,payTime,handleType,timestamp2";
    private final static String SQL = "select o.*,os.payTime,os.handleType,os.timestamp2 from " + ORDER_TABLE + " o inner join " + ORDER_STATUS_TABLE + " os on o.orderCode=os.orderCode";

    public static void main(String[] args) throws Exception {
        String serverIp = "192.168.235.12";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamQueryConfig queryConfig = new StreamQueryConfig();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStream<Order> orderStream = env.socketTextStream(serverIp, 9990, "\n").map(new OrderMap());
        DataStream<OrderStatus> orderStatusStream = env.socketTextStream(serverIp, 9991, "\n").map(new OrderStatusMap());
        tableEnv.registerDataStream(ORDER_TABLE, orderStream, ORDER_FIELDS);
        tableEnv.registerDataStream(ORDER_STATUS_TABLE, orderStatusStream, ORDER_STATUS_FIELDS);
        Table table = tableEnv.sqlQuery(SQL);
//        tableEnv.toRetractStream(table, OrderDetail.class).print();
        tableEnv.toRetractStream(table, OrderDetail.class)
                .map(i -> i.f1)
                .keyBy(OrderDetail::getOrderCode)
                .filter(new TwoJoinFilterByTimestamp("orderDetail"))
                .print();
        env.execute();
    }
}
