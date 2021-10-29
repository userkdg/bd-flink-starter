package cn.com.bd.hive;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HiveWrite {


    private static String KAFKA_BOOTSTRAP_SERVER = "192.168.235.51:9092,192.168.235.52:9092,192.168.235.53:9092";

    private static String CREATE_HIVE_PARTITION_TABLE =
            "CREATE TABLE hive_partition_table (\n" +
                    "  user_id STRING,\n" +
                    "  order_amount DOUBLE\n" +
                    ") PARTITIONED BY (dt STRING, hr STRING,mm STRING) STORED AS parquet TBLPROPERTIES (\n" +
                 /*   "  'partition.time-extractor.kind'='custom',\n" +
                    "  'partition.time-extractor.class'='cn.com.bd.hive.MyPartTimeExtractor',\n" +
                   */ "  'partition.time-extractor.timestamp-pattern'='$dt $hr:$mm:00',\n" +
                    "  'sink.rolling-policy.rollover-interval'='1 min',"+
                    "  'sink.partition-commit.trigger'='partition-time',\n" +
                    "  'sink.partition-commit.delay'='0 s',\n" +
                    "  'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
                    ")";


    public static void main(String[] args) throws Exception {
        //初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);
        tableEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "10 s");
        tableEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60000);
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        String hiveConfDir = getHiveConfDir(args);
        useCategory(tableEnv, "hiveee", hiveConfDir);
        createKafkaTable(tableEnv);
        writeHivePartitionTable(tableEnv);
        //writeHiveTable(tableEnv);
        //streamingWriteHiveTable(tableEnv);
    }

    private static String getHiveConfDir(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        return parameterTool.get("hive_conf_dir", "/etc/alternatives/hive-conf");
    }

    private static void useCategory(TableEnvironment tableEnv, String cateName, String hiveConfDir) {
        String defaultDatabase = "default";
        HiveCatalog hive = new HiveCatalog(cateName, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(cateName, hive);
        tableEnv.useCatalog(cateName);
    }

    private static void createKafkaTable(TableEnvironment tableEnv) {
        tableEnv.executeSql("drop table if exists kafka_table");
        String kafkaTable = "CREATE TABLE kafka_table (\n" +
                "  user_id STRING,\n" +
                "  order_amount DOUBLE,\n" +
                "  log_ts TIMESTAMP(3),\n" +
                "  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND\n" +
                ")WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'test_dim_kafka_123',\n" +
                " 'properties.bootstrap.servers' = '" + KAFKA_BOOTSTRAP_SERVER + "',\n" +
                " 'properties.group.id' = 'testGroup2',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset'" +
                ")";

        tableEnv.executeSql(kafkaTable);
    }

    private static void writeHiveTable(TableEnvironment tableEnv) {
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        /* tableEnv.executeSql("INSERT INTO  hive_write_test SELECT * from hive_read_test ");*/
        tableEnv.executeSql("INSERT OVERWRITE hive_write_test SELECT * from hive_read_test  ");
    }


    private static void streamingWriteHiveTable(TableEnvironment tableEnv) {
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("INSERT INTO  hive_write_test SELECT * from hive_read_test /*+ OPTIONS('streaming-source.enable'='true') */");
    }

    private static void writeHivePartitionTable(TableEnvironment tableEnv) {
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("drop table if exists hive_partition_table");
        tableEnv.executeSql(CREATE_HIVE_PARTITION_TABLE);
        tableEnv.executeSql("INSERT INTO  hive_partition_table " +
                "SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH'), DATE_FORMAT(log_ts, 'mm') FROM kafka_table");
    }


}

