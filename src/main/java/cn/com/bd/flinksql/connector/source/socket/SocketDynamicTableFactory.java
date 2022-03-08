package cn.com.bd.flinksql.connector.source.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

/**
 * @author admin
 * @createdAt 2021-05-13 15:39
 * @description socket动态表工厂类
 */
public final class SocketDynamicTableFactory implements DynamicTableSourceFactory {

    // 定义所有必要参数
    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname").stringType().noDefaultValue();

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port").intType().noDefaultValue();

    public static final ConfigOption<Integer> BYTE_DELIMITER =
            ConfigOptions.key("byte-delimiter").intType().defaultValue(10); // 对应 '\n'

    @Override
    public String factoryIdentifier() {
        return "socket"; // with 中connector的value
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(FactoryUtil.FORMAT); // 使用预定义选项格式
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BYTE_DELIMITER);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // 使用helper实现逻辑验证
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        // 设置合适的解码格式
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);

        // 验证所有的选项
        helper.validate();

        // 获取已验证的选项
        final ReadableConfig options = helper.getOptions();
        final String hostname = options.get(HOSTNAME);
        final int port = options.get(PORT);
        final byte byteDelimiter = (byte) (int) options.get(BYTE_DELIMITER);

        // 从catalog生成逻辑数据类型
        final DataType producedDataType =
                context.getCatalogTable().getSchema().toPhysicalRowDataType();

        // 创建并返回动态source表
        return new SocketDynamicTableSource(
                hostname, port, byteDelimiter, decodingFormat, producedDataType);
    }
}
