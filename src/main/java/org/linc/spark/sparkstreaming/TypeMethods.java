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
        Set<Class<?>> ret = new HashSet<Class<?>>();
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
        if (typeStr.equals("int")) {
            return Integer.class;
        } else if (typeStr.equals("Integer")) {
            return int.class;
        } else if (typeStr.equals("long")) {
            return Long.class;
        } else if (typeStr.equals("Long")) {
            return long.class;
        } else if (typeStr.equals("double")) {
            return Double.class;
        } else if (typeStr.equals("Double")) {
            return double.class;
        } else if (typeStr.equals("float")) {
            return Float.class;
        } else if (typeStr.equals("Float")) {
            return float.class;
        } else if (typeStr.equals("Boolean")) {
            return boolean.class;
        } else if (typeStr.equals("boolean")) {
            return Boolean.class;
        } else if (typeStr.equals("Character")) {
            return char.class;
        } else if (typeStr.equals("char")) {
            return Character.class;
        } else if (typeStr.equals("Byte")) {
            return byte.class;
        } else if (typeStr.equals("byte")) {
            return Byte.class;
        } else if (typeStr.equals("Void")) {
            return void.class;
        } else if (typeStr.equals("void")) {
            return Void.class;
        } else if (typeStr.equals("Short")) {
            return short.class;
        } else if (typeStr.equals("short")) {
            return Short.class;
        } else if (typeStr.equals("String")) {
            return String.class;
        } else {
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
        } catch (NoSuchMethodException e) {
            cause = e;
        } catch (InvocationTargetException e) {
            cause = e;
        } catch (IllegalAccessException e) {
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
