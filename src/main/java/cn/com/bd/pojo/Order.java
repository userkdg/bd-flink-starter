package cn.com.bd.pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class Order implements Serializable {
    private int orderCode;
    private float payment;
    private String platform;
    private long timestamp1 = System.currentTimeMillis();

    public Order() {

    }

    public Order(int orderCode, float payment, String platform) {
        this.orderCode = orderCode;
        this.payment = payment;
        this.platform = platform;
    }

}
