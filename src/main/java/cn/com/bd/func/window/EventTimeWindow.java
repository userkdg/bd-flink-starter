package cn.com.bd.func.window;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.text.ParseException;
import java.time.Duration;

/**
 *
 * 窗口形式[start window time, end window time)，左闭右开，窗口的时间戳是到毫秒级别的。
 * 窗口示例：窗口大小为10s的滚动窗口，如[2021-4-21 10:00:00.000, 2021-4-21 10:00:10.000)，等价于[2021-4-21 10:00:00.000, 2021-4-21 10:00:09.999]
 *         假设允许延时2s，下述的描述中都以这个示例为主。
 * 窗口创建：当属于该窗口的第一个数据元素到达时，立即创建
 * 窗口移除：当watermark >= window end timestamp + allowed lateness - 1，以窗口[2021-4-21 10:00:00.000, 2021-4-21 10:00:10.000)，延时2s
 *         为例，即，watermark >= 2021-4-21 10:00:11.999
 * 窗口触发计算：
 *         1.当watermark >= window end timestamp - 1时，发生main firing，触发窗口的第一次计算，如watermark >= 2021-4-21 10:00:09.999；
 *         2.在1已经发生的情况下，新到来的event timestamp仍是属于这个窗口内，即，2021-4-21 10:00:00.000 <= event timestamp < 2021-4-21 10:00:10.000，
 *           并且窗口还没有到达移除的条件，那么，此时，对于新来的数据，都会触发一次窗口计算，每新来一条，都是如此；
 *         3.在什么条件下会把延迟的数据能旁路输出呢，即，当watermark >= window end timestamp + allowed lateness - 1时，对于延时的数据，都会
 *           发到侧流中，如对于窗口[2021-4-21 10:00:00.000, 2021-4-21 10:00:10.000)，允许延时2s，假设当前watermark = 2021-4-21 10:00:11.999，那么，
 *           当新到过一条事件时间是2021-4-21 10:00:05的数据时，那么，这条数据不会触发窗口的计算，只会旁路输出。
 *         4.对上述计算的总结:
 *           a. 当第一次watermark >= window end timestamp - 1时，会首次触发整个窗口的计算；
 *           b. 在窗口已触发过计算的前提下，当watermark ε [window end timestamp - 1, window end timestamp + allowed lateness - 1)时，每新到一条该窗口的数据，都会触发一次窗口的完整计算；
 *           c. 当watermark >= window end timestamp + allowed lateness - 1时，每到达一条该窗口的数据，都不会触发窗口计算，但可以通过侧流获取该延迟的数据；
 *
 * 1.在socket服务器上执行命令：
 * nc -l 9995
 *
 * 2.输入数据：
 1,10,2021-04-21 10:00:02
 1,5,2021-04-21 10:00:05
 1,50,2021-04-21 10:00:10
 1,6,2021-04-21 10:00:11
 1,7,2021-04-21 10:00:12
 1,8,2021-04-21 10:00:04
 1,20,2021-04-21 10:00:15
 1,9,2021-04-21 10:00:03
 1,20,2021-04-21 10:00:22
 *
 */
public class EventTimeWindow {
    private static String SERVER_IP = "192.168.235.12";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<OrderBean> input = createDataSource(env);
        DataStream<OrderBean> orderStream = input.keyBy(OrderBean::getId)
        .assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        // 对于有多个分区的流，要记得设置withIdleness，因为，有可能存在部分分区是一直空闲的，否则的话，就算有数据
                        // 产生的那个分区的watermark会随数据一直往上涨，但空闲分区的watermark一直都没有变化，这样的话，可能会导致
                        // window不会触发任务计算，因为，window的watermark一直都不会变化。
                        .withIdleness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<OrderBean>) (orderBean, var) -> {
                            try {
                                long timestamp = DateUtils.parseDate(orderBean.getPaytime(), "yyyy-MM-dd HH:mm:ss").getTime();
                                return timestamp;
                            } catch (ParseException e) {
                                e.printStackTrace();
                                return -1;
                            }
                        })
        );

        final OutputTag<OrderBean> lateOutputTag = new OutputTag<OrderBean>("late", TypeInformation.of(OrderBean.class));
        SingleOutputStreamOperator<Tuple2<String, Double>> windowStream = orderStream.keyBy(OrderBean::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(lateOutputTag)
                .process(new MyProcessWindowFunction());
        windowStream.print();
        windowStream.getSideOutput(lateOutputTag).map(item -> {
            return "侧流数据，" + item;
        }).print();
        env.execute();
    }

    private static DataStream<OrderBean> createDataSource(StreamExecutionEnvironment env) throws Exception {
        return env.socketTextStream(SERVER_IP, 9995, "\n")
                .map(line -> {
                    String[] strArr = line.split(",");
                    OrderBean bean = new OrderBean();
                    bean.setId(StringUtils.trim(strArr[0]));
                    bean.setPayment(NumberUtils.toDouble(strArr[1]));
                    bean.setPaytime(StringUtils.trim(strArr[2]));
                    System.out.println("产生数据：" + bean);
                    return bean;
                }).returns(TypeInformation.of(OrderBean.class));
    }

    public static class MyProcessWindowFunction
            extends ProcessWindowFunction<OrderBean, Tuple2<String, Double>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<OrderBean> input, Collector<Tuple2<String, Double>> out) {
            System.out.println("当前watermark：" + context.currentWatermark() + ", " + new DateTime(context.currentWatermark()).toString("yyyy-MM-dd HH:mm:ss.SSS"));
            double sum = 0;

            for (OrderBean in: input) {
//                System.out.println(in);
                sum += in.getPayment();
            }

            String windowStart = new DateTime(context.window().getStart()).toString("yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = new DateTime(context.window().getEnd()).toString("yyyy-MM-dd HH:mm:ss.SSS");
            String outputKey = "[" + windowStart + "-" + windowEnd + ") : " + key;
            out.collect(new Tuple2<>(outputKey, sum));
        }

    }

    @Data
    private static class OrderBean implements Serializable {
        private String id;
        private double payment;
        private String paytime;
    }
}


