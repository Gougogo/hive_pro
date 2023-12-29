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
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.io.Serializable;
import java.util.stream.Collectors;

@Description(name = "array_distinct",
        value = "FUNC(n0) - Creates an union of array with the given arrays ")
public class GenericUDFArrayDistinct extends GenericUDF implements Serializable {
    private transient ListObjectInspector inputOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Check if at least two arguments were passed
        if (arguments.length != 1 || arguments[0].getCategory()!= ObjectInspector.Category.LIST) {
            throw new UDFArgumentLengthException(
                    "The function array_distinct(array) takes only 1 argument. And it should be list type");
        }
        inputOI = (ListObjectInspector) arguments[0];
        return ObjectInspectorFactory.getStandardListObjectInspector(inputOI.getListElementObjectInspector());
    }

    private static class GenericObject implements Comparable<GenericObject> {
        private final Object object;
        private final ObjectInspector oi;

        public GenericObject(Object object, ObjectInspector oi) {
            this.object = object;
            this.oi = oi;
        }

        public Object getObject() {
            return object;
        }

        public ObjectInspector getOi() {
            return oi;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof GenericObject) {
                return this.compareTo((GenericObject) o)==0;
            }  else {
                return false;
            }
        }

        @Override
        public int compareTo(GenericObject o) {
            return ObjectInspectorUtils.compare(this.object,this.oi,o.getObject(),o.getOi());
        }

        @Override
        public int hashCode() {
            return ObjectInspectorUtils.hashCode(object,oi);
        }
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return inputOI.getList(arguments[0].get())
                .stream()
                .map(x->new GenericObject(x,inputOI.getListElementObjectInspector()))
                .distinct()
                .map(GenericObject::getObject)
                .collect(Collectors.toList());
    }

    @Override
    public String getDisplayString(String[] args) {
        return getStandardDisplayString("array_distinct", args);
    }
}