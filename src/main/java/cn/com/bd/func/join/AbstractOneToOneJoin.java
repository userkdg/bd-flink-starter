package cn.com.bd.func.join;


import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;

import java.util.function.BiFunction;

/**
 * 一对一连接抽象类
 * @param <T> 数据源1的数据类型
 * @param <R> 数据源2的数据类型
 * @param <J> join生成的数据类型
 * @author 王仁线
 */
public abstract class AbstractOneToOneJoin<T, R, J> extends CoProcessFunction<T, R, J> {
    protected BiFunction<T, R, J> func; // 生成join数据的函数
    protected String state1Name; // 数据源1的状态名称
    protected String state2Name; // 数据源2的状态名称
    protected Class<T> class1; // 数据源1状态元素的数据类型
    protected Class<R> class2; // 数据源2状态元素的数据类型
    protected Time state1TTL = Time.days(1); // 数据源1状态数据TTL，默认1天
    protected Time state2TTL = Time.days(1); // 数据源2状态数据TTL，默认1天

    public AbstractOneToOneJoin(BiFunction<T, R, J> iFunc, String iState1Name, Class<T> iClass1, String iState2Name, Class<R> iClass2) {
        this.func = iFunc;
        this.state1Name = iState1Name;
        this.state2Name = iState2Name;
        this.class1 = iClass1;
        this.class2 = iClass2;
    }

    public AbstractOneToOneJoin(BiFunction<T, R, J> iFunc, String iState1Name, Class<T> iClass1, String iState2Name, Class<R> iClass2, Time iState1TTL, Time iState2TTL) {
        this(iFunc, iState1Name, iClass1, iState2Name, iClass2);
        this.state1TTL = iState1TTL;
        this.state2TTL = iState2TTL;
    }

}
