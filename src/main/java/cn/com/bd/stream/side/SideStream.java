package cn.com.bd.stream.side;

import cn.com.bd.pojo.Order;
import cn.com.bd.utils.StreamCreater;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 *
 */
public class SideStream {
    public static void main( String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Order> orderStream = StreamCreater.createOrderStream(env);
        final OutputTag<Order> jdTag = new OutputTag<Order>("jd", TypeInformation.of(Order.class));
        final OutputTag<Order> tmTag = new OutputTag<Order>("tm", TypeInformation.of(Order.class));

        SingleOutputStreamOperator<Order> mainStream = orderStream.process(new ProcessFunction<Order, Order>() {

            @Override
            public void processElement(Order order, Context context, Collector<Order> collector) throws Exception {
                if (order.getPlatform().equals("京东")) {
                    context.output(jdTag, order);
                } else if (order.getPlatform().equals("天猫")) {
                    context.output(tmTag, order);
                } else if (order.getPlatform().equals("拼多多")) {
                    collector.collect(order);
                }
            }
        });

        mainStream.print();

        DataStream<Order> jdStream = mainStream.getSideOutput(jdTag);
        jdStream.map(order -> {
            order.setPlatform("京东-tag");
            return order;
        }).print();

        DataStream<Order> tmStream = mainStream.getSideOutput(tmTag);
        tmStream.map(order -> {
            order.setPlatform("天猫-tag");
            return order;
        }).print();

        env.execute();
    }
}
