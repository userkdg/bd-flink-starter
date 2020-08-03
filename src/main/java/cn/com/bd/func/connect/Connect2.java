package cn.com.bd.func.connect;

import cn.com.bd.func.map.OrderMap;
import cn.com.bd.func.map.OrderStatusMap;
import cn.com.bd.pojo.Order;
import cn.com.bd.pojo.OrderDetail;
import cn.com.bd.pojo.OrderStatus;
import cn.com.bluemoon.bd.flink.func.join.OneToOneJoin;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
 *
 * orderStatus:（9991上输入）
 * 1,2020-03-28 09:10:00,1
 * 1,2020-03-28 09:20:00,2
 * 1,2020-03-28 09:30:00,3
 * 1,2020-03-28 09:40:00,4
 *
 */
public class Connect2 {

    public static void main(String[] args) throws Exception {
        String serverIp = "192.168.235.12";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Order> orderStream = env.socketTextStream(serverIp, 9990, "\n").map(new OrderMap());
        DataStream<OrderStatus> orderStatusStream = env.socketTextStream(serverIp, 9991, "\n").map(new OrderStatusMap());
        DataStream<OrderDetail> detailStream = orderStream.connect(orderStatusStream)
                .keyBy(Order::getOrderCode, OrderStatus::getOrderCode)
                .process(new OneToOneJoin(new OrderDetailFunc(), "order", Order.class, "status", OrderStatus.class))
                .returns(OrderDetail.class);
        detailStream.print();
        env.execute();
    }
}
