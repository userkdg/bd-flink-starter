package cn.com.bd.func.process;

import cn.com.bd.pojo.OrderDispatch;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author admin
 * @createAt 2020-09-22 15:01
 * @description output分流
 */
public class Process extends ProcessFunction<OrderDispatch, OrderDispatch> {
    public final static OutputTag<OrderDispatch> IS_SEND = new OutputTag<OrderDispatch>("is_send") {
    };
    public final static OutputTag<OrderDispatch> NO_SEND = new OutputTag<OrderDispatch>("no_send") {
    };

    @Override
    public void processElement(OrderDispatch orderDispatch, Context context, Collector<OrderDispatch> collector) throws Exception {
        if (orderDispatch.getSendTime() == 2) {
            context.output(IS_SEND, orderDispatch);
        } else {
            context.output(NO_SEND, orderDispatch);
        }
    }
}
