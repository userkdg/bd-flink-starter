package cn.com.bd.cdc.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class MysqlCDCEcOmsOrder {
    public static void main(String[] args) throws Exception {
        // 构建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(60000);
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, build);

        // 创建MySQL CDC表
        tableEnv.executeSql(Constants.REGISTER_CDC_TABLE);

        TableResult tableResult = tableEnv.executeSql("select * from express limit 10");
        CloseableIterator<Row> res = tableResult.collect();
        res.forEachRemaining((row -> {
            System.out.println(row);
        }));

        tableEnv.execute("Test MySQL CDC");
    }
}
