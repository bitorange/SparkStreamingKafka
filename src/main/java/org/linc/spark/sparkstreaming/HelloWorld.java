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
      /*  String a ="sql=SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM input";
        String b[] = a.split("=");
        for(int i = 0 ; i < b.length; i++){
            System.out.println(b[i]);
        }
        String c = "a";
        if(c == "a")
            System.out .println("1");
        else if (c.equals("a"))
            System.out.println("2");
        else
            System.out.print("3");*/
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
        try {
            // 获取规则
            Rules rules = new Rules(inputAndOutputFormat);
            String input = "1.1.1.1\tGET\t/apps/logs/LogAnalyzer.java HTTP/1.1\t200\t1000";
            LinkedHashMap<String, String> inputValue = inputAndOutputFormat.splitInput(input);

            // 应用规则到每一个字段上
            ArrayList<Object> result = rules.applyRules(inputValue);
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
    }
}
