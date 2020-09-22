package cn.com.bd.func.map;

import cn.com.bd.pojo.Order;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class OrderMap implements MapFunction<String, Order> {

    @Override
    public Order map(String line) throws Exception {
        String[] strArr = line.split(",");
        Order order = new Order(NumberUtils.toInt(strArr[0]), NumberUtils.toFloat(strArr[1]), strArr[2]);
        return order;
    }

}
