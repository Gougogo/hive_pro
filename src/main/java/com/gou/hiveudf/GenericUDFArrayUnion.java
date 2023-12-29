package com.gou.hiveudf;

/**
 * @version 1.0
 * @auther GouMi
 */
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.io.Serializable;
import java.util.ArrayList;

@Description(name = "array_union",
        value = "FUNC(n0, n1...) - Creates an union of array with the given arrays ")
public class GenericUDFArrayUnion extends GenericUDF implements Serializable {
    private transient ListObjectInspector[] inputOI;

    private final transient ArrayList<Object> result = new ArrayList<>();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Check if at least two arguments were passed
        if (arguments.length < 2 || arguments[0] == null) {
            throw new UDFArgumentLengthException(
                    "The function array_union(array, array...) takes at least 2 arguments. And the first array cannot be null");
        }

        inputOI = new ListObjectInspector[arguments.length];

        ObjectInspector baseListElementOI = ((ListObjectInspector) arguments[0]).getListElementObjectInspector();

        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] == null) {
                continue;
            }

            // Check if two argument is of category LIST
            if (!arguments[i].getCategory().equals(ObjectInspector.Category.LIST)) {
                throw new UDFArgumentTypeException(i,
                        "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
                                + "expected at function array_union, but "
                                + "\"" + arguments[i].getTypeName() + "\" "
                                + "is found");
            }
            ListObjectInspector thisListOI = (ListObjectInspector) arguments[i];
            ObjectInspector thisListElementOI = thisListOI.getListElementObjectInspector();
            // Check if two array are of same type
            if (!ObjectInspectorUtils.compareTypes(thisListElementOI, baseListElementOI)) {
                throw new UDFArgumentTypeException(1,
                        "\"" + baseListElementOI.getTypeName() + "\""
                                + " expected at function array_concat, but "
                                + "\"" + thisListElementOI.getTypeName() + "\""
                                + " is found");
            }

            inputOI[i] = ((ListObjectInspector) arguments[i]);
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(baseListElementOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        result.clear();
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] == null) {
                continue;
            }
            Object sourceArray = arguments[i].get();
            for (int j =0; j< inputOI[i].getListLength(sourceArray); j++) {
                Object arrayElement = inputOI[i].getListElement(sourceArray,j);
                result.add(arrayElement);
            }
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] args) {
        return getStandardDisplayString("array_union", args);
    }
}