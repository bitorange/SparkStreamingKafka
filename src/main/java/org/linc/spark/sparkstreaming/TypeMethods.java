package org.linc.spark.sparkstreaming;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;

/**
 * 存储与 Class / Type 相关的一些方法
 * Created by ihainan on 4/16/15.
 */
public class TypeMethods {
    private static final Set<Class<?>> WRAPPER_TYPES = getWrapperTypes();   // 判断一个类是否是包装类

    /**
     * 判断一个类是否是包装类 + String 类
     *
     * @param clazz 需要查询的类
     * @return true 为包装类或者 String 类，否则不是
     */
    public static boolean isWrapperType(Class<?> clazz) {
        return WRAPPER_TYPES.contains(clazz);
    }

    private static Set<Class<?>> getWrapperTypes() {
        Set<Class<?>> ret = new HashSet<>();
        ret.add(String.class);
        ret.add(Boolean.class);
        ret.add(Character.class);
        ret.add(Byte.class);
        ret.add(Short.class);
        ret.add(Integer.class);
        ret.add(Long.class);
        ret.add(Float.class);
        ret.add(Double.class);
        ret.add(Void.class);
        return ret;
    }

    /**
     * 根据字符串获得某类型所对应的基本类或者包装类
     *
     * @param typeStr 表示类型的字符串，例如 String，不需要加包名
     * @return 类型所对应的基本类或者包装类
     * @throws ClassNotFoundException 找不到字符串表示的类
     */
    public static Class getCorrespondingDataTypeByString(String typeStr) throws ClassNotFoundException {
        switch (typeStr) {
            case "int":
                return Integer.class;
            case "Integer":
                return int.class;
            case "long":
                return Long.class;
            case "Long":
                return long.class;
            case "double":
                return Double.class;
            case "Double":
                return double.class;
            case "float":
                return Float.class;
            case "Float":
                return float.class;
            case "Boolean":
                return boolean.class;
            case "boolean":
                return Boolean.class;
            case "Character":
                return char.class;
            case "char":
                return Character.class;
            case "Byte":
                return byte.class;
            case "byte":
                return Byte.class;
            case "Void":
                return void.class;
            case "void":
                return Void.class;
            case "Short":
                return short.class;
            case "short":
                return Short.class;
            case "String":
                return String.class;
            default:
                throw new ClassNotFoundException();
        }
    }

    /**
     * 获得某类型所对应的基本类或者包装类
     *
     * @param type 需要查询的类型
     * @return 类型所对应的基本类或者包装类
     * @throws ClassNotFoundException 找不到对应的类
     */
    public static Class getCorrespondingDataTypeByClass(Class type) throws ClassNotFoundException {
        String typeStr = type.getSimpleName();
        return getCorrespondingDataTypeByString(typeStr);
    }


    /**
     * 字符串转换成指定类型数据
     *
     * @param klazz
     * @param arg
     * @param <T>
     * @return
     */
    public static <T> T valueOf(Class<T> klazz, String arg) throws ClassNotFoundException {
        if (!TypeMethods.isWrapperType(klazz)) {
            klazz = TypeMethods.getCorrespondingDataTypeByClass(klazz);
        }
        Exception cause = null;
        T ret = null;
        try {
            ret = klazz.cast(
                    klazz.getDeclaredMethod("valueOf", String.class)
                            .invoke(null, arg)
            );
        } catch (NoSuchMethodException | IllegalAccessException e) {
            cause = e;
        } catch (InvocationTargetException e) {
            cause = e;
        }
        if (cause == null) {
            return ret;
        } else {
            throw new IllegalArgumentException(cause);
        }
    }

    /**
     * 根据类名获取对应的类
     *
     * @param typeName 类名（不含包名）
     * @return 对应的类
     * @throws ClassNotFoundException 找不到相应的类
     */
    public static Class getClassByName(String typeName) throws ClassNotFoundException {
        return TypeMethods.getCorrespondingDataTypeByClass(TypeMethods.getCorrespondingDataTypeByString(typeName));
    }
}
