package org.linc.spark.sparkstreaming;

/**
 * Created by ihainan on 4/12/15.
 */
public class MyFunctions {
    public static String toUpperCase(String str){
        return str.toUpperCase();
    }

    public static int addOne(Integer number){
        return number + 1;
    }

    public static int add(Integer number1, int number2){
        return number1 + number2;
    }

    public static String add(String str1, String str2){
        return str1 + " " + str2;
    }

    public static String getOriginalStr(String str){
        return str;
    }
}