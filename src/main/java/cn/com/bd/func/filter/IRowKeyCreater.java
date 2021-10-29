package cn.com.bd.func.filter;

import java.io.Serializable;

/**
 * @author 刘天能
 * @createAt 2021-02-05 9:16
 * @description
 */
public interface IRowKeyCreater<T> extends Serializable {
    String create(T row);
}
