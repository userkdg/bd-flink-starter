package cn.com.bd.func.map;

import cn.com.bd.pojo.OrderDispatch;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author 刘天能
 * @createAt 2020-07-06 15:30
 * @description
 */
public class OrderDispatchMap implements MapFunction<String, OrderDispatch> {
    @Override
    public OrderDispatch map(String s) throws Exception {
        String[] lineStr = s.split(",");
        return new OrderDispatch(lineStr[0], lineStr[1], NumberUtils.toInt(lineStr[2]), NumberUtils.toInt(lineStr[3]));
    }
}
