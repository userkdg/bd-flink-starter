package cn.com.bd.func.connect;

import cn.com.bd.pojo.Order;
import cn.com.bd.pojo.OrderDetail;
import cn.com.bd.pojo.OrderStatus;
import cn.com.bd.utils.ListStateCreater;
import cn.com.bd.utils.ValueStateCreater;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class OrderCoProcessFunc extends CoProcessFunction<Order, OrderStatus, OrderDetail> {
    private ListState<Order> ordersState;
    private ValueState<OrderStatus> statusState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.ordersState = ListStateCreater.create("orderState", Time.minutes(5), Order.class, this.getRuntimeContext());
        this.statusState = ValueStateCreater.create("statusState", Time.minutes(5), OrderStatus.class, this.getRuntimeContext());
    }

    @Override
    public void processElement1(Order order, Context context, Collector<OrderDetail> collector) throws Exception {
        OrderStatus status = this.statusState.value();

        if (null == status) {
            this.ordersState.add(order);
            return;
        }

        collector.collect(this.createOrderDetail(order, status));
    }

    @Override
    public void processElement2(OrderStatus status, Context context, Collector<OrderDetail> collector) throws Exception {
        this.statusState.update(status);
        Iterable<Order> iterable = this.ordersState.get();

        if (null != iterable) {
            Iterator<Order> iter = iterable.iterator();
            Order order = null;

            while (iter.hasNext()) {
                order = iter.next();
                collector.collect(this.createOrderDetail(order, status));
            }

            if (null != order) {
                this.ordersState.clear();
            }
        }
    }

    private OrderDetail createOrderDetail(Order order, OrderStatus status) {
        OrderDetail detail = new OrderDetail();
        detail.setOrderCode(order.getOrderCode());
        detail.setPlatform(order.getPlatform());
        detail.setPayment(order.getPayment());
        detail.setPayTime(status.getPayTime());
        detail.setHandleType(status.getHandleType());
        detail.setTimestamp1(order.getTimestamp1());
        detail.setTimestamp2(status.getTimestamp2());
        return detail;
    }

}
