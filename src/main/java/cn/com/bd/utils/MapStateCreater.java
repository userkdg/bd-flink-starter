package cn.com.bd.utils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

public class MapStateCreater<UK, UV> {

    /**
     * @param name           state的名称
     * @param time           state的ttl时间
     * @param keyClass       state key的类型
     * @param valueClass     state value的类型
     * @param runtimeContext
     * @param <UK>
     * @param <UV>
     * @return
     */
    public static <UK, UV> MapState create(String name, Time time, Class<UK> keyClass, Class<UV> valueClass, RuntimeContext runtimeContext) {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(time).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).cleanupFullSnapshot().neverReturnExpired().build();
        MapStateDescriptor<UK, UV> stateDescriptor = new MapStateDescriptor<>(name, keyClass, valueClass);
        stateDescriptor.enableTimeToLive(ttlConfig);
        return runtimeContext.getMapState(stateDescriptor);
    }
}
