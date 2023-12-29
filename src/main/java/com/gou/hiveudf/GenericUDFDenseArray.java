package com.gou.hiveudf;

/**
 * @version 1.0
 * @auther GouMi
 */
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.Serializable;
import java.util.ArrayList;

@Description(name = "dense_array",
        value = "FUNC(n0, n1...) - Creates an array with the given elements, also ignores null value ")
public class GenericUDFDenseArray extends GenericUDF implements Serializable {
    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver =
                new GenericUDFUtils.ReturnObjectInspectorResolver(true);

        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] == null
                    || arguments[i].getCategory() == ObjectInspector.Category.PRIMITIVE
                    && ((PrimitiveObjectInspector)arguments[i]).getPrimitiveCategory()== PrimitiveObjectInspector.PrimitiveCategory.VOID) {
                continue;
            }
            if (!returnOIResolver.update(arguments[i])) {
                throw new UDFArgumentTypeException(i,
                        "The expressions after dense_array should all have the same type: \""
                                + returnOIResolver.get().getTypeName()
                                + "\" is expected but \"" + arguments[i].getTypeName()
                                + "\" is found");
            }
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];

        ObjectInspector returnOI =
                returnOIResolver.get(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] == null
                    || arguments[i].getCategory() == ObjectInspector.Category.PRIMITIVE
                    && ((PrimitiveObjectInspector)arguments[i]).getPrimitiveCategory()== PrimitiveObjectInspector.PrimitiveCategory.VOID) {
                continue;
            }
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    returnOI);
        }


        return ObjectInspectorFactory.getStandardListObjectInspector(returnOIResolver.get());
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
        ArrayList<Object> ret = new ArrayList<>();
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] == null) {
                continue;
            }
            Object ai = arguments[i].get();
            if (ai != null && converters[i]!=null ) {
                ret.add(converters[i].convert(arguments[i].get()));
            }
        }
        return ret;

    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("dense_array", children, ",");
    }
}