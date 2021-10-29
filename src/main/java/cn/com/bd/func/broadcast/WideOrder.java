package cn.com.bd.func.broadcast;

import lombok.Data;

import java.io.Serializable;

@Data
public class WideOrder implements Serializable {
    private int orderCode;
    private float payment;
    private String platform;
    private String type;
}
