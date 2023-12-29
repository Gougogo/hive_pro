package com.gou.hiveudf;

/**
 * @version 1.0
 * @auther GouMi
 */
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GenericUDFExtractAddress extends GenericUDF implements Serializable {
    private static class Area {
        private final String province;
        private final String town;
        private final String district;
        private final String areaCode;
        private final String areaType;

        public Area(String province, String town, String district, String areaCode, String areaType) {
            this.province = province;
            this.town = town;
            this.district = district;
            this.areaCode = areaCode;
            this.areaType = areaType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Area area = (Area) o;
            return areaCode.equals(area.areaCode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(areaCode);
        }

    }

    private transient List<Area> areaList;
    private transient ObjectInspector strObjectInspector;
    private static final Pattern cleanUpPattern = Pattern.compile("[^\\u4e00-\\u9fa5\\uFF10-\\uFF19]");
    private static final String COUNTRY_PREFIX = "(^|中国)";
    private static final String PROVINCE_SUFFIX = "(省|市|自治区|壮族自治区|回族自治区|维吾尔自治区|特别行政区|行政区)";

    private boolean isTypeCompatible(ObjectInspector argument) {
        PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) argument);
        return
                poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING ||
                        poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.CHAR ||
                        poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VARCHAR;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "extract_address() accepts exactly 1 arguments.");
        }

        strObjectInspector = arguments[0];


        if (!isTypeCompatible(strObjectInspector)) {
            throw new UDFArgumentTypeException(0, "The first " +
                    "argument of function extract_address must be a string, " +
                    "char or varchar but " +
                    strObjectInspector.toString() + " was given.");
        }

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null) {
            return null;
        }

        String address = ObjectInspectorUtils.copyToStandardJavaObject(
                arguments[0].get(), strObjectInspector).toString();

        return evaluate(address);
    }


    @Override
    public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
        super.copyToNewInstance(newInstance); // Asserts the class invariant. (Same types.)
        GenericUDFExtractAddress that = (GenericUDFExtractAddress) newInstance;
        if (that != this) {
            that.areaList = (this.areaList == null ? null : new ArrayList<>(this.areaList));
            that.strObjectInspector = this.strObjectInspector;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("extract_address", children);
    }

    private void loadResource() {
        areaList = new ArrayList<>(5000);
        try (InputStream in = GenericUDFExtractAddress.class.getClassLoader()
                .getResourceAsStream("area_map.csv");
             BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
        ) {
            br.lines().forEach(l -> {
                StringTokenizer st = new StringTokenizer(l, ",");
                if (st.hasMoreTokens()) {
                    areaList.add(new GenericUDFExtractAddress.Area(st.nextToken(), st.nextToken(), st.nextToken(), st.nextToken(), st.nextToken()));
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String evaluate(String address) {
        if (address == null) {
            return null;
        }

        if (areaList == null) {
            loadResource();
        }

        Optional<String> areaCode = Optional.empty();

        address = cleanUpAddress(address);
        String province = getProvince(address);

        if (!StringUtils.isEmpty(province)) {
            areaCode = findArea(province, null, null, "P").stream().map(x -> x.areaCode).findAny();
        }

        String town = getTown(address, province);

        if (!StringUtils.isEmpty(town)) {
            // means town can be found under that province
            Optional<String> townCode = findArea(province, town, null, "T").stream().map(x -> x.areaCode).findAny();
            if (townCode.isPresent()) {
                areaCode = townCode;
            }
        }

        String district = getDistrict(address, province, town);

        if (!district.isEmpty()) {
            Optional<String> districtCode = findArea(province, town, district, "D").stream().map(x -> x.areaCode).findAny();
            if (districtCode.isPresent()) {
                areaCode = districtCode;
            }
        }

        return areaCode.orElse(null);
    }

    /**
     * Remove special characters
     *
     * @param address address in chinese to be cleanedup
     * @return cleaned up address
     */
    private String cleanUpAddress(String address) {
        return cleanUpPattern.matcher(address).replaceAll("");
    }


    /**
     * Simple lookup function based on different keys
     *
     * @param province
     * @param town
     * @param district
     * @param areaType
     * @return
     */
    private List<Area> findArea(String province, String town, String district, String areaType) {
        return areaList.stream()
                .filter(x -> (StringUtils.isEmpty(district) || district.equals(x.district))
                        && (StringUtils.isEmpty(town) || town.equals(x.town))
                        && (StringUtils.isEmpty(province) || province.equals(x.province))
                        && (StringUtils.isEmpty(areaType) || areaType.equals(x.areaType)))
                .collect(Collectors.toList());
    }

    private String getProvince(String address) {
        Pattern p = Pattern.compile(COUNTRY_PREFIX + "("
                + StringUtils.join(findArea("", null, null, "P").stream().map(x -> x.province).distinct().collect(Collectors.toList()), "|")
                + ")");
        Matcher m = p.matcher(address);
        String province = "";
        if (m.find()) {
            province = m.group(2);
        }
        return province;
    }

    /**
     * Generate Province Pattern. It will contain two groups
     *
     * @param province
     * @return
     */
    private String generateProvincePattern(String province) {
        if (StringUtils.isEmpty(province)) {
            return "(())";
        } else {
            return "(" + province + PROVINCE_SUFFIX + "?)";
        }
    }

    private String getTown(String address, String province) {
        String town = "";
        Pattern p = Pattern.compile(COUNTRY_PREFIX
                + generateProvincePattern(province)
                + "("
                // town
                + StringUtils.join(findArea(province, null, null, "T").stream().map(x -> x.town).distinct().collect(Collectors.toList()), "|")
                + ")(?!路)(?!.路)");
        Matcher m = p.matcher(address);
        if (m.find()) {
            town = m.group(4);
        }
        return town;
    }

    private String generateTownPattern(String town) {
        if (StringUtils.isEmpty(town)) {
            return "()";
        } else {
            return "(" + town + ".?)";
        }
    }

    private String getDistrict(String address, String province, String town) {
        String district = "";
        Pattern p = Pattern.compile(COUNTRY_PREFIX
                + generateProvincePattern(province)
                + generateTownPattern(town)
                + "("
                + StringUtils.join(findArea(province, town, null, "D").stream().map(x -> x.district).distinct().collect(Collectors.toList()), "|")
                + ")");
        Matcher m = p.matcher(address);
        if (m.find()) {
            district = m.group(5);
        }
        return district;
    }
}