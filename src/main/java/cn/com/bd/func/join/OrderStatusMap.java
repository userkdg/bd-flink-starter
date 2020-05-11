package cn.com.bd.func.join;

import cn.com.bd.pojo.Order;
import cn.com.bd.pojo.OrderStatus;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class OrderStatusMap implements MapFunction<String, OrderStatus> {

    @Override
    public OrderStatus map(String line) throws Exception {
        String[] strArr = line.split(",");
        return new OrderStatus(NumberUtils.toInt(strArr[0]), strArr[1], NumberUtils.toInt(strArr[2]));
    }

}
