package cn.com.bd.cep;

import cn.com.bd.func.map.OrderDispatchMap;
import cn.com.bd.pojo.OrderDispatch;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 1.在socket服务器上执行命令：
 * nc -l 9990
 * <p>
 * 2.数据：
 * OrderDispatch:（9990上输入）
 * 12345ew,2020-06-07,1,1
 * 12345ew,2020-06-07,2,1
 * 12345ews,2020-06-07,1,1
 * 12345ews,2020-06-07,1,1
 * 12345ewsw,2020-06-07,2,1
 * <p>
 * 操作
 * 可以根据自己的需求调整within()的时间大小
 * 因为服务器上不能敲入null的数值，故以1来代替，实际情况一般判断时间为null
 * 按设定的超时时间范围在服务器上敲入数据，观察数据的变化
 * 如果想看全流程数据，可以执行singleOutputStreamOperator.print()
 */

/**
 * @author admin
 * @createAt 2020-07-06 15:13
 * @description Flink CEP 学习demo
 */
public class CepProcess {
    public static void main(String[] args) throws Exception {
        //服务器ip
        String serverIp = "192.168.243.21";
        //创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置水印，为定时任务执行而设
        final AssignerWithPeriodicWatermarks extractor = new IngestionTimeExtractor<OrderDispatch>();

        //读取socket数据并且转换为keyedStream
        DataStream<OrderDispatch> dispatchDataStream = env.socketTextStream(serverIp, 9990, "\n")
                .map(new OrderDispatchMap())
                .assignTimestampsAndWatermarks(extractor)
                .keyBy(new KeySelector<OrderDispatch, String>() {
                    @Override
                    public String getKey(OrderDispatch orderDispatch) throws Exception {
                        return orderDispatch.getLogisticsOrderCode();
                    }
                });

        //构建Pattern匹配,设置超时时间为10s
        Pattern<OrderDispatch, ?> pattern = Pattern.<OrderDispatch>begin("start")
                .where(new SimpleCondition<OrderDispatch>() {
                    @Override
                    public boolean filter(OrderDispatch orderDispatch) throws Exception {
                        return orderDispatch.getSendTime() == 1;
                    }
                }).followedBy("end")
                .where(new SimpleCondition<OrderDispatch>() {
                    @Override
                    public boolean filter(OrderDispatch orderDispatch) throws Exception {
                        return orderDispatch.getSendTime() != 1;
                    }
                }).within(Time.seconds(10));

        //构建pattern流
        PatternStream<OrderDispatch> patternStream = CEP.pattern(dispatchDataStream, pattern);

        //构建超时分流器
        OutputTag<OrderDispatch> timeOut = new OutputTag<OrderDispatch>("timeOut") {
        };

        //将正常流与超时流进行分离
        SingleOutputStreamOperator<OrderDispatch> singleOutputStreamOperator = patternStream.flatSelect(
                timeOut,
                new PatternFlatTimeoutFunction<OrderDispatch, OrderDispatch>() {
                    @Override
                    public void timeout(Map<String, List<OrderDispatch>> map, long l, Collector<OrderDispatch> collector) throws Exception {
                        map.values().iterator().next().forEach(collector::collect);
                    }
                },
                new PatternFlatSelectFunction<OrderDispatch, OrderDispatch>() {
                    @Override
                    public void flatSelect(Map<String, List<OrderDispatch>> map, Collector<OrderDispatch> collector) throws Exception {
                        map.values().iterator().next().forEach(collector::collect);
                    }
                }
        );

        //利用OutputTag将超时的数据流分流出来
        DataStream<OrderDispatch> sideOutput = singleOutputStreamOperator.getSideOutput(timeOut);
        sideOutput.print();

        env.execute("CEP学习");
    }
}
