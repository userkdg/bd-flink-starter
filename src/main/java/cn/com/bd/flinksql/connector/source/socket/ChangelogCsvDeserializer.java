package cn.com.bd.flinksql.connector.source.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.regex.Pattern;

/**
 * @author 刘天能
 * @createdAt 2021-05-13 15:59
 * @description changelog数据格式序列化
 */
public final class ChangelogCsvDeserializer implements DeserializationSchema<RowData> {

    private final List<LogicalType> parsingTypes;
    private final DynamicTableSource.DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;
    private final String columnDelimiter;

    public ChangelogCsvDeserializer(
            List<LogicalType> parsingTypes,
            DynamicTableSource.DataStructureConverter converter,
            TypeInformation<RowData> producedTypeInfo,
            String columnDelimiter) {
        this.parsingTypes = parsingTypes;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        // 返回FLink核心接口所需要的的类型信息
        return producedTypeInfo;
    }

    @Override
    public void open(InitializationContext context) {
        // 转换器必须打开
        converter.open(RuntimeConverter.Context.create(ChangelogCsvDeserializer.class.getClassLoader()));
    }

    @Override
    public RowData deserialize(byte[] message) {
        // 解析包含变更日志的列
        final String[] columns = new String(message).split(Pattern.quote(columnDelimiter));
        final RowKind kind = RowKind.valueOf(columns[0]);
        final Row row = new Row(kind, parsingTypes.size());
        for (int i = 0; i < parsingTypes.size(); i++) {
            row.setField(i, parse(parsingTypes.get(i).getTypeRoot(), columns[i + 1]));
        }
        // 转换为FLink内部主句结构
        return (RowData) converter.toInternal(row);
    }

    /**
     * 逻辑类型转换
     * @param root 逻辑数据类型
     * @param value 数据类型
     * @return 转换后的类型
     */
    private static Object parse(LogicalTypeRoot root, String value) {
        switch (root) {
            case INTEGER:
                return Integer.parseInt(value);
            case VARCHAR:
                return value;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }
}
