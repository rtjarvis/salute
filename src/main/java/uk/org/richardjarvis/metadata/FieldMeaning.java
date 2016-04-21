package uk.org.richardjarvis.metadata;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Contains the semantic meaning of the field. This is assessed automatically by the values in the field.
 */
public class FieldMeaning {

    private String matchingRegex;
    private MeaningType meaningType;
    private String format;
    private DataType type;

    public FieldMeaning(MeaningType meaningType, String format, DataType type) {
        this.meaningType=meaningType;
        this.format=format;
        this.type=type;
    }

    public FieldMeaning(MeaningType meaningType, String format, DataType type, String matchingRegex) {
        this.meaningType = meaningType;
        this.format = format;
        this.type = type;
        this.matchingRegex=matchingRegex;
    }

    public String getMatchingRegex() {
        return matchingRegex;
    }

    public void setMatchingRegex(String matchingRegex) {
        this.matchingRegex = matchingRegex;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FieldMeaning{");
        sb.append("meaningType=").append(meaningType);
        sb.append(", format='").append(format).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public MeaningType getMeaningType() {
        return meaningType;
    }

    public void setMeaningType(MeaningType meaningType) {
        this.meaningType = meaningType;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public DateTimeFormatter getDateFormattter() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(getFormat()).withZone(getZone());
        return dateTimeFormatter;
    }

    public ZoneId getZone() {
        return ZoneId.systemDefault();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldMeaning that = (FieldMeaning) o;

        if (!(getType() != null ? getType().equals(that.getType()) : that.getType() == null)) return false;

        if(getMeaningType()==MeaningType.UNKNOWN || that.getMeaningType()==MeaningType.UNKNOWN) return true;

        if (getMeaningType() != that.getMeaningType()) return false;
        if (getFormat() != null ? !getFormat().equals(that.getFormat()) : that.getFormat() != null) return false;

        return true;

    }

    @Override
    public int hashCode() {
        int result = getMeaningType() != null ? getMeaningType().hashCode() : 0;
        result = 31 * result + (getFormat() != null ? getFormat().hashCode() : 0);
        result = 31 * result + (getType() != null ? getType().hashCode() : 0);
        return result;
    }

    public enum MeaningType {PHONE_NUMBER, MAC_ADDRESS, IPv4, IPv6, EMAIL_ADDRESS, URL, UNKNOWN, GEOHASH, AUDIO_WAVEFORM, NUMERIC, TEXT, DATE, DATE_PART, ORGANISATION, LOCATION, MONEY, NAME, BOOLEAN, IPv4_SUBNET, POSTAL_CODE, LATITUDE, LONGITUDE, LOCALE, CONTINENT_CODE, CONTINENT, COUNTRY_NAME, COUNTRY, RAW_IMAGE_DATA, IMAGE_METADATA, HSL_IMAGE_DATA, ONE_HOT}

}
