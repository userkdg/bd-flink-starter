package cn.com.bd.func.iterative;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterativeApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> someIntegers = env.generateSequence(0, 1000);
        IterativeStream<Long> iteration = someIntegers.iterate();
        DataStream<Long> minusOne = iteration.map(value -> value - 1);
        DataStream<Long> stillGreaterThanZero = minusOne.filter(value -> value > 0);
        iteration.closeWith(stillGreaterThanZero);
        DataStream<Long> lessThanZero = minusOne.filter(value -> value <= 0);
        lessThanZero.print();
        env.execute();
    }

}
