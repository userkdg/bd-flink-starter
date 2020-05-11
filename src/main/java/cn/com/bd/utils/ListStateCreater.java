package cn.com.bd.utils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

public class ListStateCreater {

    /**
     * @param name           state的名称
     * @param time           state的ttl时间
     * @param typeClass      state的值类型
     * @param runtimeContext
     * @param <UK>
     * @return
     */
    public static <UK> ListState create(String name, Time time, Class<UK> typeClass, RuntimeContext runtimeContext) {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(time).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).cleanupFullSnapshot().neverReturnExpired().build();
        ListStateDescriptor<UK> stateDescriptor = new ListStateDescriptor<>(name, typeClass);
        stateDescriptor.enableTimeToLive(ttlConfig);
        return runtimeContext.getListState(stateDescriptor);
    }
}
