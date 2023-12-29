package com.gou.hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

/**
 * 将数组格式的字符串 转成 整型数组
 * @author tzq
 */
public final class StringToArray extends UDF {

    private static final String NULL_STRING = "null";

    /**
     * 如果想最后hive的数据格式是struct<>, 返回值是Integer[]的。
     * @param sourceText     :源字符串
     * @return
     */
    public List<Integer> evaluate(String sourceText) {
        if (isBlank(sourceText)) {
            return null;
        }
        if (NULL_STRING.equalsIgnoreCase(sourceText)) {
            return null;
        }

        String[] arr1 = sourceText.replace("[","").replace("]","").split(",");
        //Integer[] arr2 = new Integer[arr1.length];
        List<Integer> list = new ArrayList<>();

        for(int i = 0; i < arr1.length; i++) {
            list.add(Integer.parseInt(arr1[i]));
        }

        return list;
    }

    public static boolean isBlank(String str) {
        int strLen;
        if (str != null && (strLen = str.length()) != 0) {
            for(int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(str.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }

}
