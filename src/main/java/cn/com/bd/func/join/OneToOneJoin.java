package cn.com.bd.func.join;


import cn.com.bd.utils.ListStateCreater;
import cn.com.bd.utils.ValueStateCreater;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.function.BiFunction;

/**
 * 两个数据流的一对一连接，具体特性：
 * 1.当两个流还没连接上时，已经产生数据的流（假设为流1）的所有已到达的数据都存储在列表中，
 *   当另一个流（假设为流2）数据出现时，那么，将会与流1列表中的数据依次进行关联并输出到下流；
 * 2.两个流产生关联后，后续不论是哪个流有新的数据进来，那么，都会与另一个流当前最新的数据进行关联；
 *
 * 示例：
 * 假设有两个需要关联的数据流order（字段order_code、payment）、order_status（字段order_code、status），
 * 关于order_code=1的订单数据，两个表的数据轨迹，如下：
 * 1.依次有两条order_status数据到达，数据为{order_code=1, status=0}、{order_code=1, status=1}，
 *   并且此时还没有order数据到达；
 * 2.有一条order数据到达，数据为{order_code=1, payment=50}
 * 3.有一条order_status数据到达，数据为{order_code=1, status=3}
 * 4.有一条order数据到达，数据为{order_code=1, payment=100}
 *
 * 那么，join关联出来的数据依次为：
 * {order_code=1, status=0, payment=50}
 * {order_code=1, status=1, payment=50}
 * {order_code=1, status=3, payment=50}
 * {order_code=1, status=3, payment=100}
 *
 * @param <T> 数据源1的数据类型
 * @param <R> 数据源2的数据类型
 * @param <J> join生成的数据类型
 * @author 王仁线
 *
 */
public class OneToOneJoin<T, R, J> extends AbstractOneToOneJoin<T, R, J> {
    private ValueState<T> valueState1; // 存储数据源1当前最新的数据
    private ValueState<R> valueState2; // 存储数据源2当前最新的数据
    private ListState<T> listState1; // 当数据源2还没有数据关联时，存储数据源1已经到达的数据
    private ListState<R> listState2; // 当数据源1还没有数据关联时，存储数据源2已经到达的数据

    public OneToOneJoin(BiFunction<T, R, J> iFunc, String iState1Name, Class<T> iClass1, String iState2Name, Class<R> iClass2) {
        super(iFunc, iState1Name, iClass1, iState2Name, iClass2);
    }

    public OneToOneJoin(BiFunction<T, R, J> iFunc, String iState1Name, Class<T> iClass1, String iState2Name, Class<R> iClass2, Time iState1TTL, Time iState2TTL) {
        super(iFunc, iState1Name, iClass1, iState2Name, iClass2, iState1TTL, iState2TTL);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.valueState1 = ValueStateCreater.create(this.state1Name + "Value", this.state1TTL, this.class1, this.getRuntimeContext());
        this.valueState2 = ValueStateCreater.create(this.state2Name + "Value", this.state2TTL, this.class2, this.getRuntimeContext());
        this.listState1 = ListStateCreater.create(this.state1Name + "List", this.state1TTL, this.class1, this.getRuntimeContext());
        this.listState2 = ListStateCreater.create(this.state2Name + "List", this.state2TTL, this.class2, this.getRuntimeContext());
    }

    @Override
    public void processElement1(T element1, Context context, Collector<J> collector) throws Exception {
        R element2 = this.valueState2.value();

        if (null != element2) {
            // 不为null，说明流2的数据已经存在，那么，直接关联输出
            collector.collect(this.func.apply(element1, element2));
            this.valueState1.update(element1); // 更新流1的最新数据
            return;
        }

        Iterable<R> iterable = this.listState2.get();

        if (null != iterable) {
            Iterator<R> iter = iterable.iterator();
            R item = null;

            while (iter.hasNext()) {
                item = iter.next();
                collector.collect(this.func.apply(element1, item));
            }

            if (null == item) {
                // 说明流2还没有数据，那么将流1当前到达的数据存储在列表中
                this.listState1.add(element1);
            } else {
                /**
                 * 说明流2已经有数据并且已经与之关联输出，那么，还要做如下处理：
                 * 1.将已关联输出的流2列表中的状态数据清理掉；
                 * 2.更新流1的最新数据；
                 * 3.更新流2的最新数据；
                 */
                this.listState2.clear();
                this.valueState1.update(element1);
                this.valueState2.update(item);
            }
        } else {
            // 说明流2还没有数据，那么将流1当前到达的数据存储在列表中
            this.listState1.add(element1);
        }
    }

    @Override
    public void processElement2(R element2, Context context, Collector<J> collector) throws Exception {
        T element1 = this.valueState1.value();

        if (null != element1) {
            // 不为null，说明流1的数据已经存在，那么，直接关联输出
            collector.collect(this.func.apply(element1, element2));
            this.valueState2.update(element2); // 更新流2的最新数据
            return;
        }

        Iterable<T> iterable = this.listState1.get();

        if (null != iterable) {
            Iterator<T> iter = iterable.iterator();
            T item = null;

            while (iter.hasNext()) {
                item = iter.next();
                collector.collect(this.func.apply(item, element2));
            }

            if (null == item) {
                // 说明流1还没有数据，那么将流2当前到达的数据存储在列表中
                this.listState2.add(element2);
            } else {
                /**
                 * 说明流1已经有数据并且已经与之关联输出，那么，还要做如下处理：
                 * 1.将已关联输出的流1列表中的状态数据清理掉；
                 * 2.更新流1的最新数据；
                 * 3.更新流2的最新数据；
                 */
                this.listState1.clear();
                this.valueState1.update(item);
                this.valueState2.update(element2);
            }
        } else {
            // 说明流1还没有数据，那么将流2当前到达的数据存储在列表中
            this.listState2.add(element2);
        }
    }
}