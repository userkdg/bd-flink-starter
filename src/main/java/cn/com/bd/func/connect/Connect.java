package cn.com.bd.func.connect;

import cn.com.bd.func.join.OrderMap;
import cn.com.bd.func.join.OrderStatusMap;
import cn.com.bd.pojo.Order;
import cn.com.bd.pojo.OrderDetail;
import cn.com.bd.pojo.OrderStatus;
import cn.com.bd.utils.StreamCreater;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 非window场景下两个流的join处理
 * 使用方法：
 * stream1.connect(stream2)
 *        .keyBy(s1::key(), s2::key())
 *        .process(new CoProcessFunction())
 */
public class Connect {
    public static void main( String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Order> orderStream = StreamCreater.createOrderStream(env);
        DataStream<OrderStatus> statusStream = StreamCreater.createOrderStatusStream(env);
        DataStream<OrderDetail> detailStream = orderStream.connect(statusStream)
                .keyBy(Order::getOrderCode, OrderStatus::getOrderCode)
                .process(new OrderCoProcessFunc());

//        detailStream.keyBy(OrderDetail::getPlatform)
//                .fold(new OrderResult(), new OrderFoldFunc())
//                .print();
//
//        detailStream.keyBy(OrderDetail::getOrderCode).max("payment").print();
//        detailStream.keyBy(OrderDetail::getOrderCode).minBy("payment").print();
        detailStream.keyBy(OrderDetail::getOrderCode).sum("payment").print();

        env.execute();
    }
}
