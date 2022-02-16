package cn.com.bd.utils;

import lombok.Getter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum DemoEnum implements IEnum<Integer, String> {
    A(1, "a"), B(2, "b"), C(3, "c");

    @Getter
    private final Integer value;
    @Getter
    private final String name;

    DemoEnum(Integer value, String name) {
        this.value = value;
        this.name = name;
    }

    public static void main(String[] args) {
        Map<Integer, String> valueNameMap = IEnumUtils.getValueNameMap(DemoEnum.class);
        assert valueNameMap.get(1).equals("a");
        assert valueNameMap.get(2).equals("b");
        assert valueNameMap.get(3).equals("c");
    }
}

/**
 * @author Jarod.Kong
 */
interface IEnum<K extends Serializable, V extends Serializable> {
    K getValue();

    V getName();
}

class IEnumUtils {

    @SuppressWarnings("rawtypes")
    public static <E extends Enum<?> & IEnum> E valueOf(Class<E> enumClass, int value) {
        return Arrays.stream(enumClass.getEnumConstants())
                .filter(e -> e.getValue().equals(value))
                .findFirst().orElse(null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <E extends Enum<?> & IEnum, K extends Serializable, V extends Serializable> Map<K, V> getValueNameMap(Class<E> enumClass) {
        return Arrays.stream(enumClass.getEnumConstants())
                .collect(Collectors.toMap(e -> (K) e.getValue(), e -> (V) e.getName(), (a, b) -> b));
    }
}
