package cn.com.bd.func.filter;

import cn.com.bd.utils.MapStateCreater;
import lombok.Data;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * 两个流join后的数据过滤器
 *
 * 功能：
 * flink自提供的join组件，当其中一条流的同主键的数据到达时，会关联另一条流同主键下的所有数据并发往下游，
 * 因此，需要对之前已处理过的另一个流的数据进行过滤，而这个过滤器就是为了解决这个问题。
 *
 * 目的：
 * 当一个流的新数据到达时，只需要与另一个流的最新的数据进行关联并发往下流处理。
 *
 * 实现原理：
 * 1.两个流的join，一般区分一对一连接、一对多连接，为了同时满足这两种场景，在实现时引进了主键生成器，针对一对一
 *   连接，在使用时不需要指定主键生成器，只需要使用默认的即可。
 * 2.两个流join生成的数据，需要指定两个时间戳，代表两个流数据的生成时间，在过滤时，只需要取两个时间戳都是最大的那
 *   条join数据。
 * @author 刘天能
 * @createAt 2021-02-05 9:14
 * @description
 */
public class TwoJoinFilterByTimestamp<T extends ITwoTimestamp> extends RichFilterFunction<T> {
    private static IRowKeyCreater DEFAULT_CREATER = new IRowKeyCreater() {
        @Override
        public String create(Object row) {
            return "1";
        }
    };
    // 主键生成器
    private IRowKeyCreater<T> creater;
    // 状态名称
    private String stateName;
    // 状态TTL
    private Time stateTTL;
    // 状态存储
    private MapState<String, MaxTimestamp> states;

    public TwoJoinFilterByTimestamp(String iStateName) {
        this(DEFAULT_CREATER, iStateName);
    }

    public TwoJoinFilterByTimestamp(String iStateName, Time iStateTTL) {
        this(DEFAULT_CREATER, iStateName, iStateTTL);
    }

    public TwoJoinFilterByTimestamp(IRowKeyCreater iCreater, String iStateName) {
        this(iCreater, iStateName, Time.days(1));
    }

    public TwoJoinFilterByTimestamp(IRowKeyCreater iCreater, String iStateName, Time iStateTTL) {
        this.creater = iCreater;
        this.stateName = iStateName;
        this.stateTTL = iStateTTL;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.states = MapStateCreater.create(this.stateName, this.stateTTL, String.class, MaxTimestamp.class, this.getRuntimeContext());
    }

    @Override
    public boolean filter(T element) throws Exception {
        String rowKey = this.creater.create(element);
        long curTime1 = element.getTimestamp1();
        long curTime2 = element.getTimestamp2();
        MaxTimestamp max = this.states.get(rowKey);

        if (null == max) {
            /**
             * 说明该主键的数据第一次到达
             */
            this.states.put(rowKey, new MaxTimestamp(curTime1, curTime2));
            return true;
        }

        /**
         * 如果当前到达的数据的时间都大于等于历史的最大时间，那么，该数据是新数据，需要发往下游，否则，
         * 该数据之前已到达并处理过，直接抛弃
         */
        if (max.lesser(curTime1, curTime2)) {
            max.setTimestamp1(curTime1);
            max.setTimestamp2(curTime2);
            this.states.put(rowKey, max);
            return true;
        } else {
            return false;
        }
    }

    @Data
    public static class MaxTimestamp implements Serializable {
        private long timestamp1;
        private long timestamp2;

        public MaxTimestamp() {

        }

        public MaxTimestamp(long iTimestamp1, long iTimestamp2) {
            this.timestamp1 = iTimestamp1;
            this.timestamp2 = iTimestamp2;
        }

        public boolean lesser(long curTime1, long curTime2) {
            if (curTime1 >= this.timestamp1 && curTime2 >= this.timestamp2) {
                return true;
            } else {
                return false;
            }
        }
    }
}
