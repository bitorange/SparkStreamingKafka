package org.linc.spark.sparkstreaming;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;


/**
 * 本类用于表示输入和输出格式
 */
public class InputAndOutputFormat implements Serializable {
    private static String inputFormatFilePath = "files/inputFormat.json";
    private static String outputFormatFilePath = "files/outputFormat.json";
    private static String extraSQLOutputFormatFilePath = "files/extraSQLFormat.json";
    private LinkedHashMap<String, String> inputFormat = new LinkedHashMap<String, String>();             // 输入格式，通过 字段：属性 的集合表示
    private LinkedHashMap<String, String> outputFormat = new LinkedHashMap<String, String>();            // 输出格式，通过 字段：属性 的集合表示
    private LinkedHashMap<String, String> extraSQLOutputFormat = new LinkedHashMap<String, String>();     // 额外 SQL 输出格式，通过 字段：属性 的集合表示
    private LinkedHashMap<String, String> finalOutputFormat = new LinkedHashMap<String, String>();     // 额外 SQL 输出格式，通过 字段：属性 的集合表示
    private static boolean enableExtraSQL = false;    // 是否开启对输出结果进一步执行 SQL 操作
    private static String inputLineFormat = "separator";    // 输入数据的表示格式，可以是分割符、XML 或者 JSON
    private static String separator = "\t"; // 使用的分隔符

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
     * 获取额外 SQL 输出格式
     *
     * @return 额外 SQL 输出格式
     */
    public LinkedHashMap<String, String> getExtraSQLOutputFormat() {
        return extraSQLOutputFormat;
    }

    /**
     * 获取数据最终输出格式
     *
     * @return 最终输出格式
     */
    public LinkedHashMap<String, String> getFinalOutputFormat() {
        return finalOutputFormat;
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
        LinkedHashMap<String, String> result = new LinkedHashMap<String, String>();
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
        // 获取输入格式 / 输出格式文件路径
        InputAndOutputFormat.inputFormatFilePath = GlobalVar.configMap.get("stream.input.formatFilePath");
        InputAndOutputFormat.outputFormatFilePath = GlobalVar.configMap.get("stream.output.formatFilePath");
        InputAndOutputFormat.enableExtraSQL = Boolean.valueOf(GlobalVar.configMap.get("stream.extraSQL.enable"));
        InputAndOutputFormat.inputLineFormat = GlobalVar.configMap.get("stream.input.format");
        InputAndOutputFormat.separator = GlobalVar.configMap.get("stream.input.separator");

        // 解析输入格式
        HashMap<String, String> inputFormatObj = this.parseFormatFile(InputAndOutputFormat.inputFormatFilePath);
        for (String key : inputFormatObj.keySet()) {
            inputFormat.put(key, String.valueOf(inputFormatObj.get(key)));
        }

        // 解析输出格式
        HashMap<String, String> outputFormatObj = this.parseFormatFile(InputAndOutputFormat.outputFormatFilePath);
        for (String key : outputFormatObj.keySet()) {
            outputFormat.put(key, String.valueOf(outputFormatObj.get(key)));
        }

        // 解析额外 SQL 输出格式
        if (enableExtraSQL) {
            HashMap<String, String> extraSQLOutputFormatObj = this.parseFormatFile(InputAndOutputFormat.extraSQLOutputFormatFilePath);
            for (String key : extraSQLOutputFormatObj.keySet()) {
                extraSQLOutputFormat.put(key, String.valueOf(extraSQLOutputFormatObj.get(key)));
            }
            finalOutputFormat = extraSQLOutputFormat;
        } else {
            finalOutputFormat = outputFormat;
        }
    }

    /**
     * 将输入数据转换成 LinkedHashMap
     *
     * @param inputStr 输入数据字符串
     * @return 包含输入数据的 LinkedHashMap
     * @throws ClassNotFoundException       无法将输入数据转换成对应类型
     * @throws JSONException                JSON 解析错误
     * @throws ParserConfigurationException 解析 XML 失败
     * @thrwos SAXException 解析 XML 失败
     */

    public LinkedHashMap<String, Object> splitInputIntoHashMap(String inputStr) throws JSONException, ClassNotFoundException, ParserConfigurationException, IOException, SAXException {
        LinkedHashMap<String, Object> inputValue = new LinkedHashMap<String, Object>();
        if (InputAndOutputFormat.inputLineFormat.equals("separator")) {
            String[] splitInput = inputStr.split(InputAndOutputFormat.separator);
            int i = 0;
            for (String field : inputFormat.keySet()) {
                String value = splitInput[i];
                Class type = TypeMethods.getClassByName(inputFormat.get(field));
                Object valueObj;
                if (type == String.class) {
                    valueObj = value;
                } else {
                    valueObj = TypeMethods.valueOf(type, value);
                }
                inputValue.put(field, valueObj);
                i++;
            }
        } else if (InputAndOutputFormat.inputLineFormat.equals("json")) {
            // 解析 JSON
            JSONObject obj = new JSONObject(inputStr);
            Iterator it = obj.keys();
            while (it.hasNext()) {
                Object o = it.next();
                if (o instanceof String) {
                    String field = (String) o;
                    String value = obj.get(field).toString();
                    Class type = TypeMethods.getClassByName(inputFormat.get(field));
                    Object valueObj;
                    if (type == String.class) {
                        valueObj = value;
                    } else {
                        valueObj = TypeMethods.valueOf(type, value);
                    }
                    inputValue.put(field, valueObj);
                }
            }
        } else if (InputAndOutputFormat.inputLineFormat.equals("xml")) {
            ByteArrayInputStream stream = new ByteArrayInputStream(inputStr.getBytes());
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document document = builder.parse(stream);
            NodeList nodeList = document.getElementsByTagName("entry");
            // NodeList nodeList = document.getElementsByTagName("xml");
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                String field = node.getAttributes().getNamedItem("key").getTextContent();
                String value = node.getTextContent();
                Class type = TypeMethods.getClassByName(inputFormat.get(field));
                Object valueObj;
                if (type == String.class) {
                    valueObj = value;
                } else {
                    valueObj = TypeMethods.valueOf(type, value);
                }
                inputValue.put(field, valueObj);
            }
        }

        return inputValue;
    }
}
