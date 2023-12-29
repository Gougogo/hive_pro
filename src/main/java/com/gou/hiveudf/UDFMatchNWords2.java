package com.gou.hiveudf;

/**
 * @version 1.0
 * @auther GouMi
 */
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Description(name = "matchNWords2",
        value = "FUNC(str1, str2) - Return the count of the matched words between str1 and str2")
public class UDFMatchNWords2 extends GenericUDF {
    private transient StringObjectInspector[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("AddNumbers UDF requires two arguments.");
        }

        converters = new StringObjectInspector[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = (StringObjectInspector)arguments[i];
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * return the count of match words between str1 and str2
     *
     * @return match words count
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String str1 = converters[0].getPrimitiveJavaObject(arguments[0].get());
        String str2 = converters[1].getPrimitiveJavaObject(arguments[1].get());

        if (str1 == null || str2 == null) {
            return null;
        }

        // Step1: split words
        ArrayList<String> s1 = splitWords(str1);
        ArrayList<String> s2 = splitWords(str2);

        // Step2: match n words
        // convert to HashMap
        Map<String, Integer> map1 = convertListToMap(s1);
        Map<String, Integer> map2 = convertListToMap(s2);

        int matchCnt = 0;

        for (Map.Entry<String, Integer> entry : map1.entrySet()) {
            if (map2.containsKey(entry.getKey())) {
                matchCnt += Math.min(entry.getValue(), map2.get(entry.getKey()));
            }
        }
        return matchCnt;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("add_numbers", children);
    }

    /**
     * Convert List To Map. The Key is word, and the value is the count of word.
     *
     * @param list
     * @return Map<String, Integer>
     */
    public static Map<String, Integer> convertListToMap(List<String> list) {
        return list.stream().collect(Collectors.toMap(
                String::toString, v -> 1, (v1, v2) -> (v1 + v2)
        ));
    }

    /**
     * Split String to Words. 英文以一个单词作为word, 中文以单个字作为word.
     *
     * @param src
     * @return ArrayList<String>
     */
    public static ArrayList<String> splitWords(String src) {
        // Step1: Split String to Character. Noted: Chinese takes two bytes.
        char[] charArray = src.toCharArray();

        ArrayList<String> result = new ArrayList<>();

        // Step2: split Character to words.
        for (int i = 0, j = 0, e = 0; i < charArray.length; ) {
            // TODO: splitWords可扩展第二个参数 char[] chars, 用来指定英文单词的分隔符
            //  后续这里使用StringUtils.containsAny方法做判断
            // 如果是空格(全角以及半角)或者是\t
            if (charArray[i] == '\u0020' || charArray[i] == '\u3000' || charArray[i] == '\u0009') {
                i++;
                j++;
                e = i;
            }
            // 如果是英文字符
            else if (charArray[i] <= 0x00FF) {
                j++;
                // 数组下标越界处理
                if (j >= charArray.length) {
                    StringBuilder sb = new StringBuilder();
                    for (int t = e; t < j; t++) {
                        sb.append(charArray[t]);
                    }
                    result.add(sb.toString());
                    i++;
                    e = i;
                }
                // 如果下一位也是英文字符, 除了英文空格
                else if (charArray[j] != '\u0020' && charArray[j] <= 0x00FF) {
                    i++;
                }
                // 其他情况: 中文或空格. 到此，单个英文单词分词完毕
                else {
                    StringBuilder sb = new StringBuilder();
                    for (int t = e; t < j; t++) {
                        sb.append(charArray[t]);
                    }
                    result.add(sb.toString());
                    e = i;
                    i++;
                }
            }
            // 其他情况：中文
            else {
                result.add(String.valueOf(charArray[i]));
                i++;
                j++;
                e = i;
            }
        }
        return result;
    }
}
