package cn.com.bd.pojo;

import cn.com.bluemoon.bd.flink.func.filter.ITwoTimestamp;
import lombok.Data;

import java.io.Serializable;

@Data
public class OrderDetail implements Serializable, ITwoTimestamp {
    private int orderCode;
    private float payment;
    private String payTime;
    private int handleType;
    private String platform;
    private long timestamp1;
    private long timestamp2;
}
