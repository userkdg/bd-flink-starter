package cn.com.bd.func.broadcast;

import cn.com.bd.func.map.OrderMap;
import cn.com.bd.func.map.OrderStatusMap;
import cn.com.bd.pojo.Order;
import cn.com.bd.pojo.OrderDetail;
import cn.com.bd.pojo.OrderStatus;
import cn.com.bd.pojo.Platform;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 1.广播流，可解决broadcast join场景问题。
 * 2.timeservice解决超时匹配的问题。
 *
 * 测试数据执行：
 *
 * 1.在socket服务器上执行命令（开一个shell窗口）：
 * nc -l 9990
 *
 * 2.数据：
 * orders:（9990上输入）
 * --启动后输入下述数据
 * 1,10,贝店
 * 2,20,云集
 * --1.5分钟后输入下述数据
 * 3,30,维品会
 * 4,40,天猫
 * 5,50,京东
 * 6,60,贝店
 * 7,70,云集
 */
public class BroadCast {
    public final static MapStateDescriptor<String, Platform> PLATFORM_DESCRIPTOR = new MapStateDescriptor<>(
            "platformConfig",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(Platform.class));

    public static void main( String[] args) throws Exception {
        String serverIp = "192.168.235.12";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Order> orderStream = env.socketTextStream(serverIp, 9990, "\n")
                .map(new OrderMap());

        BroadcastStream<Platform> platformStream = env.addSource(new PlatformSource()).broadcast(PLATFORM_DESCRIPTOR);
        DataStream<WideOrder> wideOrderStream = orderStream.keyBy(Order::getOrderCode)
                   .connect(platformStream)
                   .process(new OrderBroadcastProcessFunc())
                   .setParallelism(2);

        wideOrderStream.print();
        env.execute();
    }
}
