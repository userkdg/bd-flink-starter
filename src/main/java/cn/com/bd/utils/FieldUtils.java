package cn.com.bd.utils;

import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Expressions;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.reflect.FieldUtils.getAllFieldsList;

/**
 * @author 刘天能
 * @createAt 2021-02-05 9:18
 * @description
 */
public class FieldUtils {
    public static <T> String getFieldNames(Class<T> clazz) {
        return getAllFieldsList(clazz).stream()
                .map(Field::getName)
                .collect(Collectors.joining(","));
    }

    /**
     * 字符串转字段数组
     *
     * @param fields
     * @return
     */
    public static ApiExpression[] convertFieldArray(String fields) {
        return Arrays.stream(fields.split(","))
                .map(Expressions::$)
                .toArray(ApiExpression[]::new);
    }
}
