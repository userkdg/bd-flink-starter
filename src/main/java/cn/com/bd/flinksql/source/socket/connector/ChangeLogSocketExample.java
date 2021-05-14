package cn.com.bd.flinksql.source.socket.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *  操作指引
 *  1. 进入192.168.235.19的网关机
 *  2. nc -lk 9999
 *  3. 输入 INSERT|Alice|12 类型数据回车
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

        tableEnv.executeSql("select * from userScore").print();
    }
}
