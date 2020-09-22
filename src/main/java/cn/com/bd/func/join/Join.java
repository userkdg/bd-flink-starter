package cn.com.bd.func.join;

import cn.com.bd.func.map.OrderMap;
import cn.com.bd.func.map.OrderStatusMap;
import cn.com.bd.pojo.Order;
import cn.com.bd.pojo.OrderDetail;
import cn.com.bd.pojo.OrderStatus;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * window join连接两个流的元素，它们共享一个公共key并位于同一个窗口中
 * 将两边的元素传递给用户定义的JoinFunction或FlatJoinFunction，用户可以在其中发出满足连接条件的结果。
 *
 * 一般用法概括如下:
 * stream.join(otherStream)
 *     .where(<KeySelector>)
 *     .equalTo(<KeySelector>)
 *     .window(<WindowAssigner>)
 *     .apply(<JoinFunction>)
 *
 * 创建两个流元素的成对组合的行为类似于内连接，如果来自一个流的元素与另一个流没有相对应要连接的元素，则不会发出该元素。
 *
 * 测试数据执行：
 *
 * 1.在socket服务器上执行命令（分别开两个shell窗口）：
 * nc -l 9990
 * nc -l 9991
 *
 * 2.数据：
 * orders:（9990上输入）
 * 1,10,京东
 * 2,20,天猫
 * 3,30,京东
 * 4,40,京东
 *
 * orderStatus:（9991上输入）
 * 1,2020-03-28 09:10:00,1
 * 2,2020-03-28 09:20:00,2
 * 3,2020-03-28 09:30:00,1
 */
public class Join {
    public static void main( String[] args) throws Exception {
        String serverIp = "192.168.235.12";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Order> orderStream = env.socketTextStream(serverIp, 9990, "\n")
                .map(new OrderMap());
        DataStream<OrderStatus> orderStatusStream = env.socketTextStream(serverIp, 9991, "\n")
                .map(new OrderStatusMap());
        orderStream.join(orderStatusStream)
                .where(Order::getOrderCode)
                .equalTo(OrderStatus::getOrderCode)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new JoinFunction<Order, OrderStatus, OrderDetail>() {
                       @Override
                       public OrderDetail join(Order order, OrderStatus orderStatus) throws Exception {
                           OrderDetail detail = new OrderDetail();
                           detail.setOrderCode(order.getOrderCode());
                           detail.setPayment(order.getPayment());
                           detail.setPlatform(order.getPlatform());
                           detail.setPayTime(orderStatus.getPayTime());
                           detail.setHandleType(orderStatus.getHandleType());
                           return detail;
                       }
                }).print();
        env.execute();
    }
}
