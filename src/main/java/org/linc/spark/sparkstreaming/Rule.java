package org.linc.spark.sparkstreaming;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * 转换规则
 * Created by ihainan on 4/12/15.
 */
public class Rule {
    /**
     * 构造函数
     *
     * @param ruleStr              规则字符串
     * @param inputAndOutputFormat 输入与输出格式
     * @throws ClassNotFoundException    无法找到输入中某字段的数据类型
     * @throws ParseRuleStrFailException 解析规则字符串失败
     */
    public Rule(String ruleStr, InputAndOutputFormat inputAndOutputFormat) throws ParseRuleStrFailException, ClassNotFoundException {
        this.parseRuleStr(ruleStr); // 解析规则
        this.inputAndOutputFormat = inputAndOutputFormat;
        this.getParametersType();   // 获取所有参数的数据类型
    }

    /**
     * 获取输入与输出数据数据格式
     *
     * @return 输入与输出数据数据格式
     */
    public InputAndOutputFormat getInputAndOutputFormat() {
        return inputAndOutputFormat;
    }

    public InputAndOutputFormat inputAndOutputFormat;

    /**
     * 输出字段名
     *
     * @return 输出字段名
     */
    public String getFieldName() {
        return fieldName;
    }

    private String fieldName;

    /**
     * 函数名
     *
     * @return 函数名
     */
    public String getFunctionName() {
        return functionName;
    }

    private String functionName;

    /**
     * 或者需要调用函数所在的类
     *
     * @return 函数所在的类
     */
    public String getClassName() {
        return className;
    }

    private String className;

    /**
     * 函数参数列表
     *
     * @return 函数参数列表
     */
    public String[] getParameters() {
        return parameters;
    }

    private String[] parameters;


    private ArrayList<Class> paramTypes = new ArrayList<>();    // 用于确定参数的类型，注意参数可能是输入，即与某输入项相同数据类型

    /**
     * 确定参数类型
     *
     * @throws ClassNotFoundException 无法找到输入中某字段的数据类型
     */
    private void getParametersType() throws ClassNotFoundException {
        // 确定参数类型
        paramTypes = new ArrayList<>();
        isFromField = new ArrayList<>();
        for (String parameter : parameters) {
            Class type = getPossibleType(parameter);
            paramTypes.add(type);
        }
    }

    /**
     * 替换函数参数中的变量为常量
     *
     * @param parametersValue 参数值
     * @param inputValue      输入字段的值
     * @return 转换后的参数值
     */
    public String[] getRealParameters(String[] parametersValue, HashMap<String, String> inputValue) {
        for (int i = 0; i < parametersValue.length; ++i) {
            String parameterValue = parametersValue[i];
            if (isFromField.get(i)) {
                parametersValue[i] = inputValue.get(parameterValue);
            }
        }
        return parametersValue;
    }

    /**
     * 应用本规则，得到结果
     *
     * @param inputValue 输入字段的值
     * @return 运算结果
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     */
    public Object applyRule(HashMap<String, String> inputValue) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // 构建 parametersValue
        String[] parametersValue = Arrays.copyOf(parameters, parameters.length);

        // 检查参数个数
        if (parametersValue.length != parameters.length) {
            System.err.println(this.getClassName() + ": 参数个数不符合应有个数");
            return null;
        }

        // 替换参数中的变量，获取真实值
        parametersValue = this.getRealParameters(parametersValue, inputValue);

        // 强制转换
        ArrayList<Object> parametersValueObj = new ArrayList<>();
        for (int i = 0; i < parametersValue.length; ++i) {
            String value = parametersValue[i];
            Object object = !paramTypes.get(i).equals(String.class) ? valueOf(paramTypes.get(i), value) : value;
            parametersValueObj.add(object);
        }

        ArrayList<ArrayList<Class>> allPossibleTypes = getPossibleTypes(new ArrayList<ArrayList<Class>>(), new ArrayList<Class>(), 0);

        // 调用方法
        try {
            Boolean isFoundMethod = false;
            Class cls = Class.forName(this.className);
            Object result = null;
            for(ArrayList<Class> possibleTypes: allPossibleTypes){
                try {
                    Method methodToInvoke = cls.getDeclaredMethod(getFunctionName(), possibleTypes.toArray(new Class[possibleTypes.size()]));
                    isFoundMethod = true;
                    result = methodToInvoke.invoke(null, parametersValueObj.toArray(new Object[parametersValueObj.size()]));
                }
                catch (NoSuchMethodException noSuchMethodException) {
                    // Do nothing
                }
                catch (Exception e){
                    // Do nothing
                }
            }
            if(!isFoundMethod){
                System.err.println(this.getFunctionName() + ": 该方法不存在");
                throw new NoSuchMethodError();
            }
            else{
                return result;
            }
        } catch (ClassNotFoundException e) {
            System.err.println(this.getClassName() + ": 该类不存在");
            throw e;
        }
    }

    /**
     * 获取所有可能说的数据类型
     *
     * @param allPossibleTypes
     * @param possibleTypes
     * @param depth
     * @return
     */
    private ArrayList<ArrayList<Class>> getPossibleTypes(ArrayList<ArrayList<Class>> allPossibleTypes, ArrayList<Class> possibleTypes, int depth) {
        // 底层，添加一种可能
        if (depth == paramTypes.size()) {
            allPossibleTypes.add(possibleTypes);
        } else {
            // 添加原数据类型
            ArrayList<Class> possibleTypesOne = new ArrayList<>(possibleTypes);
            possibleTypesOne.add(paramTypes.get(depth));
            allPossibleTypes = getPossibleTypes(allPossibleTypes, possibleTypesOne, depth + 1);

            // 添加对应的原始数据类型
            ArrayList<Class> possibleTypesTwo = new ArrayList<>(possibleTypes);
            possibleTypesTwo.add(getPrimitiveType(paramTypes.get(depth)));
            allPossibleTypes = getPossibleTypes(allPossibleTypes, possibleTypesTwo, depth + 1);
        }

        return allPossibleTypes;
    }

    /**
     * 获取一个类型的原始数据类型
     *
     * @param type 数据类型
     * @return 对应的原始数据类型
     */
    private Class getPrimitiveType(Class type) {
        if (type.equals(Integer.class)) {
            return int.class;
        }

        if (type.equals(Float.class)) {
            return float.class;
        }

        if (type.equals(Double.class)) {
            return double.class;
        }
        return null;
    }

    /**
     * 字符串转换成指定类型数据
     *
     * @param klazz
     * @param arg
     * @param <T>
     * @return
     */
    static <T> T valueOf(Class<T> klazz, String arg) {
        Exception cause = null;
        T ret = null;
        try {
            ret = klazz.cast(
                    klazz.getDeclaredMethod("valueOf", String.class)
                            .invoke(null, arg)
            );
        } catch (NoSuchMethodException e) {
            cause = e;
        } catch (IllegalAccessException e) {
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

    private ArrayList<Boolean> isFromField = new ArrayList<>(); // 用于判断一个参数是变量还是常量

    /**
     * 判断参数的类型
     *
     * @param value 参数值
     * @return 参数类型
     * @throws ClassNotFoundException 找不到输入格式中
     */
    private Class getPossibleType(String value) throws ClassNotFoundException {
        // 字符串类型
        if (value.indexOf(0) == '\"' && value.indexOf(value.length() - 1) == '\"') {
            isFromField.add(false);
            return String.class;
        }
        // 字符类型
        else if (value.indexOf(0) == '\'' && value.indexOf(value.length() - 1) == '\'') {
            isFromField.add(false);
            return Character.class;
        } else {
            // 寻找相符的字段名
            for (String fieldName : inputAndOutputFormat.getInputFormat().keySet()) {
                if (fieldName.equals(value)) {
                    // TODO: 对 int 等类型的处理
                    String typeStr = inputAndOutputFormat.getInputFormat().get(fieldName);
                    isFromField.add(true);
                    return Class.forName("java.lang." + typeStr);
                }
            }

            /* 其他常见基本类型 */
            // 布尔
            if (value.equals("true") || value.equals("false")) {
                isFromField.add(false);
                return Boolean.class;
            }

            // 整数
            try {
                Integer.parseInt(value);
                isFromField.add(false);
                return Integer.class;
            } catch (Exception e) {
                // Do nothing
            }

            // 浮点
            try {
                Float.parseFloat(value);
                isFromField.add(false);
                return Float.class;
            } catch (Exception e) {
                // Do nothing
            }

            // TODO: Date
            // TODO: Datetime

        }

        // 未知类型
        return null;
    }

    /**
     * 解析规则字符串
     *
     * @param ruleStr 规则字符串
     * @throws ParseRuleStrFailException 解析规则字符串失败
     */
    private void parseRuleStr(String ruleStr) throws ParseRuleStrFailException {
        // 分割
        String[] parts = ruleStr.split(":");
        if (parts.length != 4) {
            throw new ParseRuleStrFailException("规则字符串不符合格式要求");
        }

        // 解析
        this.fieldName = parts[0];
        this.className = parts[1];
        this.functionName = parts[2];
        this.parameters = parts[3].split(",");
    }

    /**
     * 解析规则字符串失败
     */
    public class ParseRuleStrFailException extends Exception {
        public ParseRuleStrFailException(String message) {
            super(message);
        }
    }
}
