package cn.com.bd.func.process;

import cn.com.bd.pojo.OrderDispatch;
import cn.com.bd.utils.ValueStateCreater;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author admin
 * @createAt 2020-09-22 14:24
 * @description
 */
public class KeyedProcess extends KeyedProcessFunction<String, OrderDispatch, OrderDispatch> {
    private ValueState<OrderDispatch> state;
    private ValueState<Long> time;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.state = ValueStateCreater.create("value-state", Time.days(1), OrderDispatch.class, this.getRuntimeContext());
        this.time = ValueStateCreater.create("value-time", Time.days(1), Long.class, this.getRuntimeContext());
    }

    @Override
    public void processElement(OrderDispatch orderDispatch, Context context, Collector<OrderDispatch> collector) throws Exception {
        // 获取上一次状态的值
        OrderDispatch lastValue = this.state.value();
        // 获取时间戳
        Long lastTime = this.time.value();

        // 如果当前数据发货状态为2，删除定时器, 当前数据发到下游
        if (orderDispatch.getSendTime() == 2) {
            if (null != lastValue) {
                collector.collect(orderDispatch);
                if (null != lastTime) {
                    context.timerService().deleteProcessingTimeTimer(lastTime);
                }
            }

            return;
        }

        // 数据第一次进来， 做定时且将数据发往下游
        if (null == lastValue) {
            // 获取需要定时的时间
            long onTimeTimestamp = context.timerService().currentProcessingTime() + 60000L;
            context.timerService().registerProcessingTimeTimer(onTimeTimestamp);
            collector.collect(orderDispatch);
            this.state.update(orderDispatch);
            this.time.update(onTimeTimestamp);
            return;
        }

        // 当调度时间变化， 取消定时调度，数据不发网下游
        if (!StringUtils.equals(lastValue.getDispatchTime(), orderDispatch.getDispatchTime())) {
            context.timerService().deleteProcessingTimeTimer(lastTime);
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderDispatch> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        OrderDispatch orderDispatch = this.state.value();
        this.state.update(orderDispatch);
        this.time.update(timestamp);
        out.collect(orderDispatch);

        // 继续做定时
        ctx.timerService().registerProcessingTimeTimer(timestamp + 60000L);
    }
}
