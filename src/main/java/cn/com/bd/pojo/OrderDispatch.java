package cn.com.bd.pojo;

import lombok.Data;

/**
 * @author 刘天能
 * @createAt 2020-07-06 15:25
 * @description
 */
@Data
public class OrderDispatch {
    private String logisticsOrderCode;
    private String dispatchTime;
    private int sendTime;
    private int num;

    public OrderDispatch(String logisticsOrderCode, String dispatchTime, int sendTime, int num) {
        this.logisticsOrderCode = logisticsOrderCode;
        this.dispatchTime = dispatchTime;
        this.sendTime = sendTime;
        this.num = num;
    }
}
