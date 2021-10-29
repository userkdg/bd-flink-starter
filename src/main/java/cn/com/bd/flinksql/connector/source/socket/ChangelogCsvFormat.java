package cn.com.bd.flinksql.connector.source.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

/**
 * @author 刘天能
 * @createdAt 2021-05-13 15:59
 * @description
 */
public final class ChangelogCsvFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    private final String columnDelimiter;

    public ChangelogCsvFormat(String columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context, DataType producedDataType) {

        // 为序列化提供typeInformation
        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        // DeserializationSchema 中的大部分代码对内部结构不起作用，所以在最后做一个转换
        final DynamicTableSource.DataStructureConverter converter =
                context.createDataStructureConverter(producedDataType);

        // 在运行时使用逻辑类型
        final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();

        // 创建运行时类
        return new ChangelogCsvDeserializer(
                parsingTypes, converter, producedTypeInfo, columnDelimiter);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // 创建insert和delete变更格式（也可以加入update_before和update_after）
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}
