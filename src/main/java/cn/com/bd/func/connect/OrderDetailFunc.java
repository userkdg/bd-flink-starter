package cn.com.bd.func.connect;

import cn.com.bd.pojo.Order;
import cn.com.bd.pojo.OrderDetail;
import cn.com.bd.pojo.OrderStatus;

import java.io.Serializable;
import java.util.function.BiFunction;

public class OrderDetailFunc implements BiFunction<Order, OrderStatus, OrderDetail>, Serializable {

    @Override
    public OrderDetail apply(Order order, OrderStatus orderStatus) {
        OrderDetail detail = new OrderDetail();
        detail.setOrderCode(order.getOrderCode());
        detail.setPayment(order.getPayment());
        detail.setPlatform(order.getPlatform());
        detail.setPayTime(orderStatus.getPayTime());
        detail.setHandleType(orderStatus.getHandleType());
        detail.setTimestamp1(order.getTimestamp1());
        detail.setTimestamp2(orderStatus.getTimestamp2());
        return detail;
    }
}
