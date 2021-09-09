package cn.com.bd.flinksql.temporal;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TemporalTableJoinEventTime {

    public static void main(String[] args) throws Exception {
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.创建TableEnvironment(Blink planner)
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        // 3.文件path
        String userDir = System.getProperty("user.dir");
        String ratesHistoryPath = userDir + "/data/ratesHistory.csv";
        String ratesOrderPath = userDir + "/data/rateOrder.csv";

        // 4.DDL
        String ratesHistory_ddl =
                "create table currency_rates (\n" +
                        " currency STRING,\n" +
                        " conversion_rate DECIMAL(32, 2),\n" +
                        " update_time TIMESTAMP(3),\n" +
                        " PRIMARY KEY (currency) NOT ENFORCED,\n" +
                        " WATERMARK FOR update_time AS update_time \n" +
                        ") WITH (\n" +
                        " 'connector.type' = 'filesystem',\n" +
                        " 'connector.path' = '" + ratesHistoryPath + "',\n" +
                        " 'format.type' = 'csv'\n" +
                        ")";

        tableEnvironment.executeSql(ratesHistory_ddl);

        String ratesOrder_DDL =
                "create table orders (\n" +
                        " order_id    STRING,\n" +
                        " price       DECIMAL(32,2),\n" +
                        " currency    STRING,\n" +
                        " order_time  TIMESTAMP(3),\n" +
                        " WATERMARK FOR order_time AS order_time \n" +
                        ") WITH (\n" +
                        " 'connector.type' = 'filesystem',\n" +
                        " 'connector.path' = '" + ratesOrderPath + "',\n" +
                        " 'format.type' = 'csv'\n" +
                        ")";

        tableEnvironment.executeSql(ratesOrder_DDL);


        // 6.通过SQL对表的查询，生成结果表
        String sql =
                "SELECT \n" +
                        "     order_id,\n" +
                        "     price,\n" +
                        "     orders.currency,\n" +
                        "     conversion_rate,\n" +
                        "     order_time\n" +
                        " FROM orders\n" +
                        " LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF orders.order_time\n" +
                        " ON orders.currency = currency_rates.currency";

        Table table = tableEnvironment.sqlQuery(sql);

        // 7.将table表转换为DataStream
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnvironment.toRetractStream(table, Row.class);
        retractStream.print();
        env.execute();

        /**
         * 2> (true,1,29.00,RMB,114.00,2015-01-02T00:00)
         * 2> (true,4,55.00,RMB,116.00,2015-01-21T00:00)
         * 2> (true,2,19.00,RMB,115.00,2015-01-03T00:00)
         * 2> (true,3,33.00,RMB,115.00,2015-01-11T00:00)
         */

    }

}
