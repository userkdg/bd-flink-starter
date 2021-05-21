package cn.com.bd.flinksql.connector.source.socket;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *  操作指引
 *  1. 进入192.168.235.19的网关机
 *  2. nc -lk 9999
 *  3. 输入 DELETE|Alice|12 此样例数据
 *      输入数据为三列数据，分隔符为'|'，第一列可以是INSERT/DELETE，第二列为string，第三列为int
 */
public class ChangeLogSocketExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table userScore (" +
                " name string," +
                " score int)" +
                " with (" +
                " 'connector' = 'socket'," +
                " 'hostname' = '192.168.235.19'," +
                " 'port' = '9999'," +
                " 'byte-delimiter' = '10',\n" +
                " 'format' = 'changelog-csv',\n" +
                " 'changelog-csv.column-delimiter' = '|'\n" +
                ")");

//        tableEnv.executeSql("select * from userScore").print();
        tableEnv.executeSql("create table delete_test(" +
                " rowkey string," +
                " info row<name string, score int>," +
                " primary key(rowkey) not enforced" +
                ") with (" +
                " 'connector' = 'hbase-1.4',\n" +
                " 'table-name' = 'delete_test',\n" +
                " 'zookeeper.quorum' = '192.168.235.51:2181,192.168.235.52:2181,192.168.235.53:2181')");


        tableEnv.executeSql("insert into delete_test select name as rowkey, row(name, score) from userScore");
    }
}
