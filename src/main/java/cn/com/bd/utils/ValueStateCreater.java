package cn.com.bd.utils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

public class ValueStateCreater {

    /**
     * @param name           state的名称
     * @param time           state的清除时间
     * @param typeClass      sate的值类型
     * @param runtimeContext
     * @param <T>
     * @return
     */
    public static <T> ValueState create(String name, Time time, Class<T> typeClass, RuntimeContext runtimeContext) {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(time).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).cleanupFullSnapshot().neverReturnExpired().build();
        ValueStateDescriptor<T> stateDescriptor = new ValueStateDescriptor<>(name, typeClass);
        stateDescriptor.enableTimeToLive(ttlConfig);
        return runtimeContext.getState(stateDescriptor);
    }
}
