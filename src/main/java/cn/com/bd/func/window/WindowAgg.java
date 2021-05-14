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
 * 注意：在AggregateFunction中，虽然执行效率会比较高，但存在无法获取相应key、window相关的信息，因此，其
 *      使用场景往往会受限，特别是需要一同输出key、window数据时。此时，可以考虑使用ProcessWindowFunction，
 *      可以参考ProcessFunctionWindowAgg里的样例代码。
 */
public class WindowAgg {
    private static String SERVER_IP = "192.168.235.12";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Long>> input = createDataSource(env);
        SingleOutputStreamOperator<Double> windowStream = input.keyBy(element -> element.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .aggregate(new AverageAggregate());
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

    private static class AverageAggregate implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {

        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return ((double) accumulator.f0) / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

}


