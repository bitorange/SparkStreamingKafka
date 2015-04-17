package org.linc.spark.sparkstreaming;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * 用于存储规则信息的类
 * Created by ihainan on 4/13/15.
 */
public class Rules {
    private static String ruleFile = "/Users/ihainan/tmp/newRules"; // 规则文件路径
    private InputAndOutputFormat inputAndOutputFormat;

    static {
        // 获取规则文件路径
        ruleFile = GlobalConf.rulesFilePath().get();
    }

    private ArrayList<Rule> rules = new ArrayList<>();

    /**
     * 构造函数
     * @param inputAndOutputFormat 输入 / 输出格式
     * @throws java.io.IOException 读取规则文件失败
     * @throws Rule.ParseRuleStrFailException 解析规则失败
     * @throws ClassNotFoundException 找不到规则中指定的类
     */
    public Rules(InputAndOutputFormat inputAndOutputFormat) throws IOException, Rule.ParseRuleStrFailException, ClassNotFoundException {
        this.inputAndOutputFormat = inputAndOutputFormat;
        readRules(); // 从文件中读取规则
    }

    /**
     * 从外部文件中读取规则
     * @throws java.io.IOException 读取规则文件失败
     * @throws Rule.ParseRuleStrFailException 解析规则失败
     * @throws ClassNotFoundException 找不到规则中指定的类
     */
    private void readRules() throws Rule.ParseRuleStrFailException, ClassNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader(ruleFile));
        String line;
        while ((line = br.readLine()) != null) {
            Rule rule = new Rule(line, inputAndOutputFormat);
            this.rules.add(rule);
        }
    }

    /**
     *
     * @param inputValue 输入值
     * @return 输出值
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException 找不到规则指定的方法
     * @throws IllegalAccessException
     * @throws java.lang.reflect.InvocationTargetException
     */
    public ArrayList<String> applyRules(HashMap<String, Object> inputValue) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        ArrayList<String> outputValue = new ArrayList<>();
        for(String outputField: inputAndOutputFormat.getOutputFormat().keySet()){
            // 寻找规则
            for(Rule rule: rules){
                if(rule.getFieldName().equals(outputField)){
                    // 应用规则
                    Object result = rule.applyRuleToOneLine(inputValue);
                    outputValue.add(result.toString());
                    break;
                }
            }
        }
        return outputValue;
    }

    /**
     * 输出结果转换为字符串
     * @param outputValue 输出结果
     * @return 转换得到的字符串
     */
    public static String resultToStr(ArrayList<Object> outputValue){
        // TODO: 如果结果中含有 \t 该如何处理？
        String result = "";
        for(int i = 0; i < outputValue.size(); ++i){
            result += outputValue.get(i);
            result += i == outputValue.size() - 1 ? "" +
                    "" : "\t";
        }
        return result;
    }

}
