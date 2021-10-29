package cn.com.bd.func.filter;

import java.io.Serializable;

/**
 * @author 刘天能
 * @createAt 2021-02-05 9:13
 * @description
 */
public interface ITwoTimestamp extends Serializable {
    long getTimestamp1();
    long getTimestamp2();
}
