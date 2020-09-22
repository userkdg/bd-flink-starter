package cn.com.bd.pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class Platform implements Serializable {
    private String platform;
    private String type;

    public Platform() {

    }

    public Platform(String platform, String type) {
        this.platform = platform;
        this.type = type;
    }
}
