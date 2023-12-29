package com.gou.hiveudf;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 * @auther GouMi
 */
@Description(name = "to_json",
        value = "FUNC(n0) - Creates a json string based on input ")
public class GenericUDFToJson extends GenericUDF implements Serializable {
    private transient InspectorHandler inspHandler;
    private transient JsonFactory jsonFactory;

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        try (StringWriter writer = new StringWriter();
             JsonGenerator gen = jsonFactory.createJsonGenerator(writer);
        ) {
            inspHandler.generateJson(gen, args[0].get());
            gen.flush();
            return writer.toString();
        } catch (IOException io) {
            throw new HiveException(io);
        }
    }

    @Override
    public String getDisplayString(String[] args) {
        return getStandardDisplayString("to_json", args);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentLengthException("The function to_json takes an object as an argument");
        }
        ObjectInspector oi = args[0];
        if (oi.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "The function to_json takes only complex type");
        }
        inspHandler = generateInspectorHandler(oi);
        jsonFactory = new JsonFactory();
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    private InspectorHandler generateInspectorHandler(ObjectInspector oi) {
        return null;
    }

    private interface InspectorHandler {
        void generateJson(JsonGenerator gen, Object obj) throws IOException;
    }

    private class MapInspectorHandler implements InspectorHandler {
        private final MapObjectInspector mapInspector;
        private final StringObjectInspector keyObjectInspector;
        private final InspectorHandler valueInspector;

        public MapInspectorHandler(MapObjectInspector mInsp) throws UDFArgumentException {
            mapInspector = mInsp;
            try {
                keyObjectInspector = (StringObjectInspector) mInsp.getMapKeyObjectInspector();
            } catch (ClassCastException castExc) {
                throw new UDFArgumentException("Only Maps with strings as keys can be converted to valid JSON");
            }
            valueInspector = generateInspectorHandler(mInsp.getMapValueObjectInspector());
        }

        @Override
        @SuppressWarnings("unchecked")
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartObject();
                Map<String, ?> map = (Map<String, ?>) mapInspector.getMap(obj);
                for (Map.Entry<String, ?> entry : map.entrySet()) {
                    String keyJson = keyObjectInspector.getPrimitiveJavaObject(entry.getKey());
                    gen.writeFieldName(keyJson);
                    valueInspector.generateJson(gen, entry.getValue());
                }
                gen.writeEndObject();
            }
        }
    }

    private class StructInspectorHandler implements InspectorHandler {
        private final StructObjectInspector structInspector;
        private final List<String> fieldNames;
        private final List<InspectorHandler> fieldInspectorHandlers;

        public StructInspectorHandler(StructObjectInspector insp) throws UDFArgumentException {
            structInspector = insp;
            List<? extends StructField> fieldList = insp.getAllStructFieldRefs();
            this.fieldNames = new ArrayList<>();
            this.fieldInspectorHandlers = new ArrayList<>();
            for (StructField sf : fieldList) {
                fieldNames.add(sf.getFieldName());
                fieldInspectorHandlers.add(generateInspectorHandler(sf.getFieldObjectInspector()));
            }
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            //// Interpret a struct as a map ...
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartObject();
                List<Object> structObjs = structInspector.getStructFieldsDataAsList(obj);
                for (int i = 0; i < fieldNames.size(); ++i) {
                    String fieldName = fieldNames.get(i);
                    gen.writeFieldName(fieldName);
                    fieldInspectorHandlers.get(i).generateJson(gen, structObjs.get(i));
                }
                gen.writeEndObject();
            }
        }
    }

    private class ArrayInspectorHandler implements InspectorHandler {
        private final ListObjectInspector arrayInspector;
        private final InspectorHandler valueInspector;

        public ArrayInspectorHandler(ListObjectInspector lInsp) throws UDFArgumentException {
            arrayInspector = lInsp;
            valueInspector = generateInspectorHandler(arrayInspector.getListElementObjectInspector());
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                gen.writeStartArray();
                List<?> list = arrayInspector.getList(obj);
                for (Object listObj : list) {
                    valueInspector.generateJson(gen, listObj);
                }
                gen.writeEndArray();
            }
        }
    }

    private class StringInspectorHandler implements InspectorHandler {
        private final StringObjectInspector strInspector;

        public StringInspectorHandler(StringObjectInspector insp) {
            strInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                String str = strInspector.getPrimitiveJavaObject(obj);
                gen.writeString(str);
            }
        }
    }

    private class IntInspectorHandler implements InspectorHandler {
        private final IntObjectInspector intInspector;

        public IntInspectorHandler(IntObjectInspector insp) {
            intInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null)
                gen.writeNull();
            else {
                int num = intInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class DoubleInspectorHandler implements InspectorHandler {
        private final DoubleObjectInspector dblInspector;

        public DoubleInspectorHandler(DoubleObjectInspector insp) {
            dblInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                double num = dblInspector.get(obj);
                gen.writeNumber(num);
            }
        }
    }

    private class LongInspectorHandler implements InspectorHandler {
        private final LongObjectInspector longInspector;

        public LongInspectorHandler(LongObjectInspector insp) {
            longInspector = insp;
        }

        @Override
        public void generateJson(JsonGenerator gen, Object obj) throws IOException {
            if (obj == null) {
                gen.writeNull();
            } else {
                long num = longInspector.get(obj);
                gen.writeNumber(num);
            }
        }

        private class ShortInspectorHandler implements InspectorHandler {
            private final ShortObjectInspector shortInspector;

            public ShortInspectorHandler(ShortObjectInspector insp) {
                shortInspector = insp;
            }

            @Override
            public void generateJson(JsonGenerator gen, Object obj) throws IOException {
                if (obj == null) {
                    gen.writeNull();
                } else {
                    short num = shortInspector.get(obj);
                    gen.writeNumber(num);
                }
            }
        }

        private class ByteInspectorHandler implements InspectorHandler {
            private final ByteObjectInspector byteInspector;

            public ByteInspectorHandler(ByteObjectInspector insp) {
                byteInspector = insp;
            }

            @Override
            public void generateJson(JsonGenerator gen, Object obj) throws IOException {
                if (obj == null) {
                    gen.writeNull();
                } else {
                    byte num = byteInspector.get(obj);
                    gen.writeNumber(num);
                }
            }
        }

        private class FloatInspectorHandler implements InspectorHandler {
            private final FloatObjectInspector floatInspector;

            public FloatInspectorHandler(FloatObjectInspector insp) {
                floatInspector = insp;
            }

            @Override
            public void generateJson(JsonGenerator gen, Object obj) throws IOException {
                if (obj == null) {
                    gen.writeNull();
                } else {
                    float num = floatInspector.get(obj);
                    gen.writeNumber(num);
                }
            }
        }

        private class BooleanInspectorHandler implements InspectorHandler {
            private final BooleanObjectInspector boolInspector;

            public BooleanInspectorHandler(BooleanObjectInspector insp) {
                boolInspector = insp;
            }

            @Override
            public void generateJson(JsonGenerator gen, Object obj) throws IOException {
                if (obj == null) {
                    gen.writeNull();
                } else {
                    boolean tf = boolInspector.get(obj);
                    gen.writeBoolean(tf);
                }
            }
        }

        private class BinaryInspectorHandler implements InspectorHandler {
            private final BinaryObjectInspector binaryInspector;

            public BinaryInspectorHandler(BinaryObjectInspector insp) {
                binaryInspector = insp;
            }

            @Override
            public void generateJson(JsonGenerator gen, Object obj) throws IOException {
                if (obj == null) {
                    gen.writeNull();
                } else {
                    byte[] bytes = binaryInspector.getPrimitiveJavaObject(obj);
                    gen.writeBinary(bytes);
                }
            }
        }

        private class TimestampInspectorHandler implements InspectorHandler {
            private final TimestampObjectInspector timestampInspector;
            private final DateTimeFormatter isoFormatter = ISODateTimeFormat.dateTimeNoMillis();

            public TimestampInspectorHandler(TimestampObjectInspector insp) {
                timestampInspector = insp;
            }

            @Override
            public void generateJson(JsonGenerator gen, Object obj) throws IOException {
                if (obj == null) {
                    gen.writeNull();
                } else {
                    Timestamp timestamp = timestampInspector.getPrimitiveJavaObject(obj);
                    String timeStr = isoFormatter.print(timestamp.getSeconds());
                    gen.writeString(timeStr);
                }
            }
        }

        private InspectorHandler generateInspectorHandler(ObjectInspector insp) throws UDFArgumentException {
            ObjectInspector.Category cat = insp.getCategory();
            switch (cat) {
                case MAP:
                    return new MapInspectorHandler((MapObjectInspector) insp);
                case LIST:
                    return new ArrayInspectorHandler((ListObjectInspector) insp);
                case STRUCT:
                    return new StructInspectorHandler((StructObjectInspector) insp);
                case PRIMITIVE:
                    PrimitiveObjectInspector primInsp = (PrimitiveObjectInspector) insp;
                    PrimitiveObjectInspector.PrimitiveCategory primCat = primInsp.getPrimitiveCategory();
                    switch (primCat) {
                        case STRING:
                            return new StringInspectorHandler((StringObjectInspector) primInsp);
                        case INT:
                            return new IntInspectorHandler((IntObjectInspector) primInsp);
                        case LONG:
                            return new LongInspectorHandler((LongObjectInspector) primInsp);
                        case SHORT:
                            return new ShortInspectorHandler((ShortObjectInspector) primInsp);
                        case BOOLEAN:
                            return new BooleanInspectorHandler((BooleanObjectInspector) primInsp);
                        case FLOAT:
                            return new FloatInspectorHandler((FloatObjectInspector) primInsp);
                        case DOUBLE:
                            return new DoubleInspectorHandler((DoubleObjectInspector) primInsp);
                        case BYTE:
                            return new ByteInspectorHandler((ByteObjectInspector) primInsp);
                        case BINARY:
                            return new BinaryInspectorHandler((BinaryObjectInspector) primInsp);
                        case TIMESTAMP:
                            return new TimestampInspectorHandler((TimestampObjectInspector) primInsp);
                        default:
                            throw new UDFArgumentException("Don't know how to handle object inspector " + insp);
                    }
                default:
                    throw new UDFArgumentException("Don't know how to handle object inspector " + insp);
            }
        }
    }
}