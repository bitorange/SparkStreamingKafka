package org.linc.spark.sparkstreaming;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * Created by ihainan on 4/11/15.
 */
public class InputAndOutputFormat {
    private static String inputFormatFilePath = "/Users/ihainan/tmp/inputFormat.json";
    private static String outputFormatFilePath = "/Users/ihainan/tmp/outputFormat.json";

    /**
     *  从配置文件中读取输入格式文件路径和输出和格式文件路径
     */
    static {
        inputFormatFilePath = GlobalConf.inputFormatFilePath().get();
        outputFormatFilePath = GlobalConf.outputFormatFilePath().get();
    }

    private LinkedHashMap<String, String> inputFormat = new LinkedHashMap<>();  // 输入格式，通过 字段：属性 的集合表示
    private LinkedHashMap<String, String> outputFormat = new LinkedHashMap<>(); // 输出格式，通过 字段：属性 的集合表示

    /**
     * 获取输入数据格式
     *
     * @return 输入数据格式
     */
    public LinkedHashMap<String, String> getInputFormat() {
        return inputFormat;
    }

    /**
     * 获取输出数据格式
     *
     * @return 输出数据格式
     */
    public LinkedHashMap<String, String> getOutputFormat() {
        return outputFormat;
    }

    /**
     * 从文件中读取输入 / 输出格式
     *
     * @param filePath 输入 / 输出格式文件路径
     * @return 字段名到类型的映射 LinkedHashMap
     * @throws java.io.IOException                      读取 JSON 文件失败
     * @throws org.codehaus.jettison.json.JSONException JSON 解析失败
     */
    private LinkedHashMap<String, String> parseFormatFile(String filePath) throws IOException, JSONException {
        // 读取文件
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line, jsonBody = "";
        while ((line = br.readLine()) != null) {
            jsonBody += line;
        }

        // 解析 JSON
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        JSONObject obj = new JSONObject(jsonBody);
        Iterator it = obj.keys();
        while (it.hasNext()) {
            Object o = it.next();
            if (o instanceof String) {
                String key = (String) o;
                result.put(key, obj.get(key).toString());
            }
        }
        return result;
    }

    /**
     * 构造函数
     *
     * @throws java.io.IOException                      解析输入或者输出格式 JSON 文件失败
     * @throws org.codehaus.jettison.json.JSONException 解析失败
     */
    public InputAndOutputFormat() throws IOException, JSONException {
        // TODO: 处理出现非合法类型的情况
        // 解析输入格式
        HashMap<String, String> inputFormatObj = this.parseFormatFile(inputFormatFilePath);
        for (String key : inputFormatObj.keySet()) {
            inputFormat.put(key, String.valueOf(inputFormatObj.get(key)));
        }

        // 解析输出格式
        HashMap<String, String> outputFormatObj = this.parseFormatFile(outputFormatFilePath);
        for (String key : outputFormatObj.keySet()) {
            outputFormat.put(key, String.valueOf(outputFormatObj.get(key)));
        }
    }

    /**
     * 将输入数据转换成 LinkedHashMap
     *
     * @param inputStr 输入数据字符串
     * @return 包含输入数据的 LinkedHashMap
     */
    public LinkedHashMap<String, Object> splitInputIntoHashMap(String inputStr) throws ClassNotFoundException {
        LinkedHashMap<String, Object> inputValue = new LinkedHashMap<>();
        String[] splitInput = inputStr.split("\t");
        int i = 0;
        for (String field : inputFormat.keySet()) {
            String value = splitInput[i];
            Object valueObj = TypeMethods.valueOf(TypeMethods.getClassByName(inputFormat.get(field)), value);
            inputValue.put(field, valueObj);
            i++;
        }
        return inputValue;
    }
}
