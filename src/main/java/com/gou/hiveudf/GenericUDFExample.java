package com.gou.hiveudf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

@Description(
        name="GenericUDFExample",
        value="GenericUDFExample(...) - count int or long type numbers",
        extended = "Example :\n    >select GenericUDFExample(3, 5);\n    >{numerator=3,denominator=5,percentage=60%}\n"
)
public class GenericUDFExample extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        // 判断传入的参数个数
        if(objectInspectors.length != 2){
            throw new UDFArgumentLengthException("Input Args Length Error !!!");
        }
        // 判断传入参数的类型
        // objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE 参数类型为hive原始数据类型
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)
                || !PrimitiveObjectInspector.PrimitiveCategory.INT.equals(((PrimitiveObjectInspector)objectInspectors[0]).getPrimitiveCategory())){ // 传入第一个参数类型是否为hive的Int类型
            throw new UDFArgumentException("函数第一个参数为int类型"); // 当自定义UDF参数与预期不符时，抛出异常
        }
        if (!objectInspectors[1].getCategory().equals(ObjectInspector.Category.PRIMITIVE)
                || !PrimitiveObjectInspector.PrimitiveCategory.INT.equals(((PrimitiveObjectInspector)objectInspectors[1]).getPrimitiveCategory())){ // 传入第二个参数类型是否为hive的Int类型
            throw new UDFArgumentException("函数第二个参数为int类型");
        }
        //最后返回结果的类型
        return ObjectInspectorFactory.getStandardMapObjectInspector (
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector) ;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String num1 = deferredObjects[0].get().toString();
        String num2 = deferredObjects[1].get().toString();
        return intToPrecent(num1,num2);
    }

    public Map<Text,Text> intToPrecent(String i1, String i2){
        int i = Integer.parseInt(i1);
        int j = Integer.parseInt(i2);
        double result = (double)i/j;
        // 格式化器
        DecimalFormat df = new DecimalFormat("0%");
        Map<Text,Text> ret = new HashMap();
        // 实际使用中 报错 String can not cast Text，但是在IDEA上测试没问题，IDEA上做功能逻辑测试
        // ret.put("numerator",i1);
        // ret.put("denominator",i2);
        // ret.put("percentage",df.format(result));
        ret.put(new Text("numerator"),new Text(i1));
        ret.put(new Text("denominator"),new Text(i2));
        ret.put(new Text("percentage"),new Text(df.format(result)));
        return ret;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "GOOD";
    }
}