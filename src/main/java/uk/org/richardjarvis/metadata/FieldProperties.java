package uk.org.richardjarvis.metadata;

import org.apache.spark.sql.types.*;

/**
 * Created by rjarvis on 24/02/16.
 */
public class FieldProperties implements MetaData {

    private String name;
    private int maxLength = Integer.MIN_VALUE;
    private int minLength = Integer.MAX_VALUE;
    private int count = 0;
    private long totalChars;
    private Boolean isNullable = true;

    public Boolean getNullable() {
        return isNullable;
    }

    private DataType type;
    private int uniqueValues = 0;

    public FieldProperties(String value) {

        update(value);
    }

    public DataType getType() {

        return type;
    }

    @Override
    public String toString() {
        return "FieldProperties{" +
                "name='" + name + '\'' +
                ", type=" + type.toString() +
                ", minLength=" + minLength +
                ", maxLength=" + maxLength +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public double getMeanLength() {
        return (double) totalChars / count;
    }

    public int getMinLength() {
        return minLength;
    }

    public void update(String value) {

        count++;
        totalChars += value.length();

        if (value.length() > maxLength)
            maxLength = value.length();

        if (value.length() < minLength)
            minLength = value.length();

        if (type == null || type == DataTypes.IntegerType) {
            if (value.matches("^[0-9]*$")) {
                type = DataTypes.IntegerType;
            } else if (type == null || type == DataTypes.DoubleType) {
                if (value.matches("^[-+]*[0-9]*\\.[0-9]+$")) {
                    type = DataTypes.DoubleType;
                } else {
                    type = DataTypes.StringType;
                }
            }
        }
    }

    public Object convertToType(String value) {
//        switch (type) {
//            case DataTypes.DoubleType:
//                return Double.parseDouble(value);
//            case INTEGER:
//                return Integer.parseInt(value);
//            case STRING:
//                return value;
//            default:
        return value;
//        }
    }

    public StructField getStructField() {

        return new StructField(getName(), getType(), getNullable(), Metadata.empty());

    }
}
