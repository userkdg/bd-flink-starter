package cn.com.bd.func.process;

import cn.com.bd.func.map.OrderDispatchMap;
import cn.com.bd.pojo.OrderDispatch;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
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
 * 因为服务器上不能敲入null的数值，故以数值来代替，实际情况一般判断时间为null
 * 如果发货时间不为2，则做定时调度，如果发货时间为1，则停止定时调度
 * 调度时间变化停止定时调度
 */

/**
 * @author 刘天能
 * @createAt 2020-09-22 14:20
 * @description process Function 案例
 */
public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        // 服务器ip
        String serverIp = "192.168.235.12";
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取原始流之后做定时处理
        DataStream<OrderDispatch> dataStream = env.socketTextStream(serverIp, 9990, "\n")
                .map(new OrderDispatchMap())
                .keyBy(OrderDispatch::getLogisticsOrderCode)
                .process(new KeyedProcess());

        // 将数据进行发货和未发货分流
        SingleOutputStreamOperator<OrderDispatch> process = dataStream.process(new Process());

        // 数据sendTime为2的
        process.getSideOutput(Process.IS_SEND).print();

        // 输出senTime不为2的
        process.getSideOutput(Process.NO_SEND).print();

        env.execute("process Function测试");
    }
}
