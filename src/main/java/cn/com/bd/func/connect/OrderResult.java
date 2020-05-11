package cn.com.bd.func.connect;

import lombok.Data;

import java.io.Serializable;

@Data
public class OrderResult implements Serializable {
    private String platform;
    private float payment;

    public void add(float payment) {
        this.payment += payment;
    }
}
