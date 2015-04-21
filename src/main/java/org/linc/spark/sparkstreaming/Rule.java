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
     * @throws ClassNotFoundException    无法找到输入中某字段的数据类型所对应的类
     * @throws ParseRuleStrFailException 解析规则字符串失败
     */
    public Rule(String ruleStr, InputAndOutputFormat inputAndOutputFormat) throws ParseRuleStrFailException, ClassNotFoundException {
        this.parseRuleStr(ruleStr); // 解析规则
        this.inputAndOutputFormat = inputAndOutputFormat;
        this.getParametersTypeList();   // 获取所有参数的数据类型
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

    /**
     * 输出字段名
     *
     * @return 输出字段名
     */
    public String getFieldName() {
        return fieldName;
    }


    /**
     * 调用函数名
     *
     * @return 函数名
     */
    public String getFunctionName() {
        return functionName;
    }


    /**
     * 或者需要调用函数所在的类va
     *
     * @return 函数所在的类
     */
    public String getClassName() {
        return className;
    }

    private InputAndOutputFormat inputAndOutputFormat;
    private String fieldName;
    private String className;
    private String functionName;
    private String[] parameters;    // 函数参数列表
    private ArrayList<Class> paramsTypeList = new ArrayList<Class>();    // 用于确定参数的类型，注意参数可能是输入，即与某输入项相同数据类型
    private ArrayList<Boolean> isFromInputField = new ArrayList<Boolean>(); // 用于判断一个参数是变量还是常量

    /**
     * 确定参数类型
     *
     * @throws ClassNotFoundException 无法找到输入中某字段的数据类型
     */
    private void getParametersTypeList() throws ClassNotFoundException {
        // 确定参数类型
        paramsTypeList = new ArrayList<Class>();
        isFromInputField = new ArrayList<Boolean>();
        for (String parameter : parameters) {
            Class type = convertParameterValueToBasicType(parameter);
            paramsTypeList.add(type);
        }
    }

    /**
     * 根据参数（可能是常量或者变量）获得对应的数据类型
     *
     * @param value 参数值（可能是常量或者变量）
     * @return 参数对应的数据类型
     * @throws ClassNotFoundException 找不到对应的数据类型
     */
    private Class convertParameterValueToBasicType(String value) throws ClassNotFoundException {
        // 字符串类型
        if (value.charAt(0) == 34 && value.charAt(value.length() - 1) == 34) {
            isFromInputField.add(false);
            return String.class;
        }

        // 字符类型
        else if (value.charAt(0) == 39 && value.charAt(value.length() - 1) == 39) {
            isFromInputField.add(false);
            return Character.class;
        } else {
            // 寻找相符的字段名
            for (String fieldName : inputAndOutputFormat.getInputFormat().keySet()) {
                if (fieldName.equals(value)) {
                    String typeStr = inputAndOutputFormat.getInputFormat().get(fieldName);
                    isFromInputField.add(true);
                    return TypeMethods.getClassByName(typeStr);
                }
            }

            /* 其他常见基本类型 */
            // 布尔
            if (value.equals("true") || value.equals("false")) {
                isFromInputField.add(false);
                return Boolean.class;
            }

            // TODO: 长整数、短整型、字节型
            // 整数
            try {
                Integer.parseInt(value);
                isFromInputField.add(false);
                return Integer.class;
            } catch (Exception e) {
                // Do nothing
            }

            // TODO: 单精和双精
            // 浮点
            try {
                Float.parseFloat(value);
                isFromInputField.add(false);
                return Float.class;
            } catch (Exception e) {
                // Do nothing
            }

            throw new ClassNotFoundException("找不到参数相应的类型");
        }
    }

    /**
     * 替换函数参数中的变量为常量
     *
     * @param parametersValue 参数值
     * @param inputValue      所有输入字段的值（可能是变量）
     * @return 转换后的参数值（都是常量）
     */
    public String[] getConstantValueOfParameters(String[] parametersValue, HashMap<String, Object> inputValue) {
        for (int i = 0; i < parametersValue.length; ++i) {
            String parameterValue = parametersValue[i];
            if (isFromInputField.get(i)) {
                parametersValue[i] = inputValue.get(parameterValue).toString();
            } else if (parameterValue.charAt(0) == 34 && parameterValue.charAt(parameterValue.length() - 1) == 34
                    || parameterValue.charAt(0) == 39 && parameterValue.charAt(parameterValue.length() - 1) == 39) {
                parametersValue[i] = parameterValue.substring(0, parameterValue.length() - 1).substring(1);
            }
        }
        return parametersValue;
    }

    /**
     * 应用本规则到一条数据中
     *
     * @param inputValue 输入字段的字段名以及字段相应的值（都是常量）
     * @return 运算结果
     * @throws ClassNotFoundException 找不到规则中规定的类
     * @throws NoSuchMethodException  找不到规则定规定的方法
     */
    public Object applyRuleToOneLine(HashMap<String, Object> inputValue) throws ClassNotFoundException, NoSuchMethodException {
        // 构建 parametersValue
        String[] parametersValue = Arrays.copyOf(parameters, parameters.length);

        // 检查参数个数
        if (parametersValue.length != parameters.length) {
            System.err.println(this.getClassName() + ": 参数个数不符合应有个数");
            return null;
        }

        // 替换参数中的变量，获取真实值
        parametersValue = this.getConstantValueOfParameters(parametersValue, inputValue);

        // 强制转换
        ArrayList<Object> parametersValueObj = new ArrayList<Object>();
        for (int i = 0; i < parametersValue.length; ++i) {
            String value = parametersValue[i];
            Object object = !paramsTypeList.get(i).equals(String.class) ? TypeMethods.valueOf(paramsTypeList.get(i), value) : value;
            parametersValueObj.add(object);
        }

        ArrayList<ArrayList<Class>> allPossibleTypes = getPossibleTypesOfMethodParameters(new ArrayList<ArrayList<Class>>(), new ArrayList<Class>(), 0);

        // 调用方法
        try {
            Boolean isFoundMethod = false;
            Class cls = Class.forName(this.className);
            Object result = null;
            for (ArrayList<Class> possibleTypes : allPossibleTypes) {
                try {
                    Method methodToInvoke = cls.getDeclaredMethod(getFunctionName(), possibleTypes.toArray(new Class[possibleTypes.size()]));
                    isFoundMethod = true;
                    result = methodToInvoke.invoke(null, parametersValueObj.toArray(new Object[parametersValueObj.size()]));
                } catch (NoSuchMethodException e) {
                    // Do nothing
                } catch (IllegalAccessException e) {
                    // Do nothing
                } catch (InvocationTargetException e) {
                    // Do nothing
                }
            }
            if (!isFoundMethod) {
                System.err.println(this.getFunctionName() + ": 该方法不存在");
                throw new NoSuchMethodError();
            } else {
                return result;
            }
        } catch (ClassNotFoundException e) {
            System.err.println(this.getClassName() + ": 该类不存在");
            throw e;
        }
    }

    /**
     * 使用 DFS 获取函数参数所有可能的数据类型（既可能是基本类型，也可能是包装类）
     *
     * @param allPossibleTypes 函数参数所有可能的数据类型的集合，初始元素个数为空
     * @param possibleTypes    一种可能的函数参数数据类型，初始元素个数为空
     * @param depth            当前深度
     * @return 函数参数所有可能的数据类型的集合
     * @throws ClassNotFoundException 找不到某个类型对应的基本类或者包装类
     */
    private ArrayList<ArrayList<Class>> getPossibleTypesOfMethodParameters(
            ArrayList<ArrayList<Class>> allPossibleTypes, ArrayList<Class> possibleTypes, int depth)
            throws ClassNotFoundException {
        // 底层，添加一种可能
        if (depth == paramsTypeList.size()) {
            allPossibleTypes.add(possibleTypes);
        } else {
            // 添加包装类
            ArrayList<Class> possibleTypesOne = new ArrayList<Class>(possibleTypes);
            possibleTypesOne.add(paramsTypeList.get(depth));
            allPossibleTypes = getPossibleTypesOfMethodParameters(allPossibleTypes, possibleTypesOne, depth + 1);

            // 添加对应的原始数据类型
            ArrayList<Class> possibleTypesTwo = new ArrayList<Class>(possibleTypes);
            possibleTypesTwo.add(TypeMethods.getCorrespondingDataTypeByClass(paramsTypeList.get(depth)));
            allPossibleTypes = getPossibleTypesOfMethodParameters(allPossibleTypes, possibleTypesTwo, depth + 1);
        }

        return allPossibleTypes;
    }
}
