package cn.com.bd.utils;

import cn.com.bd.pojo.Order;
import cn.com.bd.pojo.OrderStatus;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class StreamCreater {

    public static DataStream<Order> createOrderStream(StreamExecutionEnvironment env) {
        List<Order> orders = Lists.newArrayList();
        orders.add(new Order(1, 10, "京东"));
        orders.add(new Order(1, 101, "京东"));
        orders.add(new Order(2, 20, "天猫"));
        orders.add(new Order(3, 30, "拼多多"));
        orders.add(new Order(4, 40, "京东"));
        orders.add(new Order(5, 50, "京东"));
        orders.add(new Order(6, 60, "天猫"));
        orders.add(new Order(6, 601, "天猫"));
        orders.add(new Order(6, 501, "天猫"));
        return env.fromCollection(orders);
    }

    public static DataStream<OrderStatus> createOrderStatusStream(StreamExecutionEnvironment env) {
        List<OrderStatus> statusList = Lists.newArrayList();
        statusList.add(new OrderStatus(1, "2020-03-28 09:10:00", 1));
        statusList.add(new OrderStatus(2, "2020-03-28 09:11:00", 1));
        statusList.add(new OrderStatus(3, "2020-03-28 09:12:00", 2));
        statusList.add(new OrderStatus(4, "2020-03-28 09:13:00", 1));
        statusList.add(new OrderStatus(5, "2020-03-28 09:14:00", 1));
        statusList.add(new OrderStatus(6, "2020-03-28 09:16:00", 1));
        statusList.add(new OrderStatus(7, "2020-03-28 09:17:00", 2));
        return env.fromCollection(statusList);
    }

}