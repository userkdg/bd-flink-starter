package cn.com.bd.func.window;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * 1.在socket服务器上执行命令（分别开两个shell窗口）：
 * nc -l 9990
 *
 * 2.输入数据：
 * 1,10
 * 1,20
 * 1,5
 * 1,40
 *
 *
 * 注意：在ProcessWindowFunction中，虽然可以同时获得key、window、elements等数据，满足各种场景的输出，
 *      但存在执行效率较低的情况，这时候，可以考虑incremental aggregation / reduce，即，将aggregateFunction
 *      / reduceFunction跟ProcessWindowFunction结合在一起，具体见IncrementalWindowAgg样例说明。
 *
 */
public class ProcessFunctionWindowAgg {
    private static String SERVER_IP = "192.168.235.12";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Long>> input = createDataSource(env);
        SingleOutputStreamOperator<Tuple2<String, Double>> windowStream = input.keyBy(element -> element.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .process(new MyProcessWindowFunction());
        windowStream.print();
        env.execute();
    }

    private static DataStream<Tuple2<String, Long>> createDataSource(StreamExecutionEnvironment env) throws Exception {
        return env.socketTextStream(SERVER_IP, 9990, "\n")
                .map(line -> {
                    String[] strArr = line.split(",");
                    return new Tuple2<String, Long>(StringUtils.trim(strArr[0]), NumberUtils.toLong(strArr[1]));
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));
    }

    public static class MyProcessWindowFunction
            extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Double>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Double>> out) {
            long count = 0;
            double sum = 0;

            for (Tuple2<String, Long> in: input) {
                sum += in.f1;
                count++;
            }

            double average = sum / count;

            String windowStart = new DateTime(context.window().getStart()).toString("yyyy-MM-dd HH:mm:ss");
            String windowEnd = new DateTime(context.window().getEnd()).toString("yyyy-MM-dd HH:mm:ss");
            String outputKey = "[" + windowStart + "-" + windowEnd + "] : " + key;
            out.collect(new Tuple2<>(outputKey, average));
        }

    }

}


