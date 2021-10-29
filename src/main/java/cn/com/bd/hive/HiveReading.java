package cn.com.bd.hive;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class HiveReading {

    public static void main(String[] args) throws Exception {
        //初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "10 s");
        tableEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled",true);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60000);
        String hiveConfDir = getHiveConfDir(args);
        useCategory(tableEnv, "hive", hiveConfDir);
        createPrintTable(tableEnv);
        streamReadPartitoinHiveTable(tableEnv);
    }

    private static String getHiveConfDir(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        return parameterTool.get("hive_conf_dir", "/etc/alternatives/hive-conf");
    }

    private static void useCategory(StreamTableEnvironment tableEnv, String cateName, String hiveConfDir) {

        String defaultDatabase = "default";
        HiveCatalog hive = new HiveCatalog(cateName, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(cateName, hive);
        tableEnv.useCatalog(cateName);
    }

    private static void readHiveTable(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("insert into print_table " +
                " select * from hive_read_test ");
    }

    private static void streamReadHiveTable(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("insert into print_table " +
                " select * from hive_read_test /*+ OPTIONS('streaming-source.enable'='true') */");
    }

    private static void readPartitionHiveTable(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("insert into print_table " +
                " select logistics_order_code,order_pay,shop_name from hive_partition_read_test where dt = '20210204'");
    }

    private static void streamReadPartitoinHiveTable(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("insert into print_table " +
                " select logistics_order_code,order_pay,shop_name from hive_partition_read_test2 /*+ OPTIONS('streaming-source.enable'='true') */");
    }



    private static void createPrintTable(StreamTableEnvironment tableEnv) {
        String PRINT_SINK_SQL = "CREATE TABLE print_table(" +
                "  logistics_order_code STRING,\n" +
                "  order_pay DOUBLE,\n" +
                "   shop_name STRING\n" +
                ") " +
                "WITH ('connector' = 'print')\n";
        tableEnv.executeSql("drop table  if exists print_table");
        tableEnv.executeSql(PRINT_SINK_SQL);


    }


}

