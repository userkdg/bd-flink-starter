package cn.com.bd.func.connect;

import cn.com.bd.pojo.OrderDetail;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.scala.function.util.ScalaFoldFunction;

public class OrderFoldFunc implements FoldFunction<OrderDetail, OrderResult> {
    @Override
    public OrderResult fold(OrderResult result, OrderDetail detail) throws Exception {
        result.setPlatform(detail.getPlatform());
        result.add(detail.getPayment());
        return result;
    }
}
