package cn.com.bd.func.connect;

import cn.com.bd.pojo.OrderDetail;
import org.apache.flink.api.common.functions.ReduceFunction;


//public class OrderFoldFunc implements reduceFunction<OrderDetail, OrderResult> {
//    @Override
//    public OrderResult fold(OrderResult result, OrderDetail detail) throws Exception {
//        result.setPlatform(detail.getPlatform());
//        result.add(detail.getPayment());
//        return result;
//    }
//}
