package cn.com.bd.func.broadcast;

import cn.com.bd.pojo.Order;
import cn.com.bd.pojo.Platform;
import cn.com.bd.utils.ValueStateCreater;
import cn.com.bluemoon.bd.utils.DateUtils;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class OrderBroadcastProcessFunc extends KeyedBroadcastProcessFunction<Integer, Order, Platform, WideOrder> {
    private ValueState<Order> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.state = ValueStateCreater.create("orderState", Time.minutes(10), Order.class, this.getRuntimeContext());
    }

    @Override
    public void processElement(Order order, ReadOnlyContext readOnlyContext, Collector<WideOrder> collector) throws Exception {
        ReadOnlyBroadcastState<String, Platform> platforms = readOnlyContext.getBroadcastState(BroadCast.PLATFORM_DESCRIPTOR);
        Platform platform = platforms.get(order.getPlatform());

        if (null != platform) {
            WideOrder wideOrder = new WideOrder();
            wideOrder.setOrderCode(order.getOrderCode());
            wideOrder.setPayment(order.getPayment());
            wideOrder.setPlatform(order.getPlatform());
            wideOrder.setType(platform.getType());
            collector.collect(wideOrder);
        } else {
            // 关联不到platform数据，则将订单数据cache起来
            this.state.update(order);
            // 设置定时器，10秒后执行
            readOnlyContext.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 10 * 1000);
        }
    }

    @Override
    public void processBroadcastElement(Platform platform, Context context, Collector<WideOrder> collector) throws Exception {
        System.out.println("接收到广播数据：" + platform);
        context.getBroadcastState(BroadCast.PLATFORM_DESCRIPTOR).put(platform.getPlatform(), platform);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WideOrder> collector) throws Exception {
        System.out.println(DateUtils.currentTime() + "，定时触发处理数据：" + this.state.value());
        Order order = this.state.value();
        if (null == order) return;
        Platform platform = ctx.getBroadcastState(BroadCast.PLATFORM_DESCRIPTOR).get(order.getPlatform());

        if (null == platform) {
            ctx.timerService().registerProcessingTimeTimer(timestamp + 10 * 1000);
            return;
        }

        WideOrder wideOrder = new WideOrder();
        wideOrder.setOrderCode(order.getOrderCode());
        wideOrder.setPayment(order.getPayment());
        wideOrder.setPlatform(order.getPlatform());
        wideOrder.setType(platform.getType());
        System.out.println(DateUtils.currentTime() + "，超时生成wideOrder数据：" + wideOrder);
        collector.collect(wideOrder);
    }

}
