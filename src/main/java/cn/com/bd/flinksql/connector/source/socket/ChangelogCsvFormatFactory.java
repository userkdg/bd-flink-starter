package cn.com.bd.flinksql.connector.source.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author admin
 * @createdAt 2021-05-13 16:00
 * @description
 */
public final class ChangelogCsvFormatFactory implements DeserializationFormatFactory {

    // 静态定义所有选项
    public static final ConfigOption<String> COLUMN_DELIMITER =
            ConfigOptions.key("column-delimiter").stringType().defaultValue("|");

    @Override
    public String factoryIdentifier() {
        return "changelog-csv";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(COLUMN_DELIMITER);
        return options;
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {

        // 验证逻辑校验（自定义或者使用帮助器）
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        // 获取字段分隔符
        final String columnDelimiter = formatOptions.get(COLUMN_DELIMITER);

        // 创建返回格式
        return new ChangelogCsvFormat(columnDelimiter);
    }
}
