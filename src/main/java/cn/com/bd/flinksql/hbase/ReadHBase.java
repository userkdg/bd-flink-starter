package cn.com.bd.flinksql.hbase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author admin
 * @createAt 2021-02-05 9:47
 * @description
 */
public class ReadHBase {
    public static void main(String[] args) throws Exception {
        // 初始化FLink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        tableEnv.executeSql("create table hTable (\n" +
                "                    rowkey STRING,\n" +
                "                    info ROW<cus_qty STRING, province STRING, city STRING>,\n" +
                "                    PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                "                    ) with (\n" +
                "                    'connector' = 'hbase-1.4',\n" +
                "                    'sink.buffer-flush.max-rows'='1',\n" +
                "                    'sink.buffer-flush.interval'='1s',\n" +
                "                    'table-name' = 'rt_s_real_cus_count',\n" +
                "                    'zookeeper.quorum' = '192.168.235.51:2181,192.168.235.52:2181,192.168.235.53:2181'\n" +
                "                   )");

        Table table = tableEnv.sqlQuery("select rowkey, info.cus_qty, info.province, info.city " +
                " from hTable");

        tableEnv.toRetractStream(table, Row.class).print();

        env.execute();
    }
}
