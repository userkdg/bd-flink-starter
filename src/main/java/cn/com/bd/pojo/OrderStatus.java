package cn.com.bd.pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class OrderStatus implements Serializable {
    private int orderCode;
    private String payTime;
    private int handleType; // 1：原单，2：子单
    private long timestamp2 = System.currentTimeMillis();

    public OrderStatus() {

    }

    public OrderStatus(int orderCode, String payTime, int handleType) {
        this.orderCode = orderCode;
        this.payTime = payTime;
        this.handleType = handleType;
    }

}

