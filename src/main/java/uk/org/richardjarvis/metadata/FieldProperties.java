package uk.org.richardjarvis.metadata;

import org.apache.spark.sql.types.*;
import uk.org.richardjarvis.utils.field.Recogniser;

import java.io.Serializable;
import java.util.List;

/**
 * Created by rjarvis on 24/02/16.
 */
public class FieldProperties implements MetaData, Serializable {

    public static final String FORMAT_METADATA = "format";
    public static final String MEANING_METADATA = "meaning";
    private String name;
    private List<FieldMeaning> possibleMeanings = null;
    private boolean nullable = true;

    public FieldProperties(String value) {
        update(value);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FieldMeaning getMeaning() {

        for (FieldMeaning fieldMeaning : possibleMeanings) {
            if (fieldMeaning.getMeaningType() != FieldMeaning.MeaningType.TEXT)
                return fieldMeaning;
        }
        return possibleMeanings.get(0);

    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public void update(String value) {

        possibleMeanings = Recogniser.getPossibleFieldMeaningTypes(possibleMeanings, value);

    }

    public StructField getStructField() {

        DataType type = getMeaning().getType();

        MetadataBuilder metadataBuilder = new MetadataBuilder();
        String format = getMeaning().getFormat();
        if (format != null)
            metadataBuilder.putString(FORMAT_METADATA, format);
        FieldMeaning.MeaningType meaningType = getMeaning().getMeaningType();
        if (meaningType != null)
            metadataBuilder.putString(MEANING_METADATA, meaningType.name());
        return new StructField(getName(), type, isNullable(), metadataBuilder.build());

    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FieldProperties{");
        sb.append("name='").append(name).append('\'');
        sb.append(", nullable=").append(nullable);
        sb.append(", possibleMeanings={");

        for (FieldMeaning fieldMeaning : possibleMeanings) {
            sb.append(fieldMeaning.toString() + "\n");
        }
        sb.append("}\n}");
        return sb.toString();
    }
}
