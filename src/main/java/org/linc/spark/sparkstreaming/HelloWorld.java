package org.linc.spark.sparkstreaming;

import org.codehaus.jettison.json.JSONException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Created by ihainan on 4/12/15.
 */
public class HelloWorld {
    public static void main(String[] args){
       //  解析输入输出格式
        InputAndOutputFormat inputAndOutputFormat = null;
        try {
            inputAndOutputFormat = new InputAndOutputFormat();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        } catch (JSONException e) {
            e.printStackTrace();
        }

        // 创建规则用于测试
        /*
        try {
            // 获取规则
            Rules rules = new Rules(inputAndOutputFormat);
            String input = "1.1.1.1\tGET\t/apps/logs/LogAnalyzer.java HTTP/1.1\t200\t1000";
            LinkedHashMap<String, String> inputValue = inputAndOutputFormat.splitInputIntoHashMap(input);

            // 应用规则到每一个字段上
            ArrayList<String> result = rules.applyRules(inputValue);
            for(Object object: result){
                System.out.println(object);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Rule.ParseRuleStrFailException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        */
    }
}
