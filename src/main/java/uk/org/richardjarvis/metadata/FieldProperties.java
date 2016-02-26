package uk.org.richardjarvis.metadata;

/**
 * Created by rjarvis on 24/02/16.
 */
public class FieldProperties  implements MetaData{

    String name;
    int maxLength = Integer.MIN_VALUE;
    int minLength = Integer.MAX_VALUE;
    int count = 0;
    long totalChars;
    Boolean isInteger = true;
    Boolean isDecimal = true;
    int uniqueValues = 0;

    public FieldProperties(String value) {

        update(value);
    }

    @Override
    public String toString() {
        return "FieldProperties{" +
                "name='" + name + '\'' +
                ", isString=" + isString() +
                ", isDecimal=" + isDecimal() +
                ", isInteger=" + isInteger() +
                ", minLength=" + minLength +
                ", maxLength=" + maxLength +
                '}';
    }

    public String getName() {
        return name;
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

    public Boolean isInteger() {
        return isInteger;
    }

    public Boolean isDecimal() {
        return isDecimal;
    }

    public Boolean isString() {
        return !(isInteger  || isDecimal);
    }

    public void update(String value) {

        count++;
        totalChars += value.length();

        if (value.length() > maxLength)
            maxLength = value.length();

        if (value.length() < minLength)
            minLength = value.length();

        if (isInteger == null || (isInteger && !isDecimal)) {
            isInteger = value.matches("^[0-9]*$");

        }

        if (isDecimal == null || isDecimal) {
            isDecimal = value.matches("^[0-9]*\\.[0-9]+$");
        }

    }


    public void setName(String name) {
        this.name = name;
    }
}
