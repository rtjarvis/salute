package uk.org.richardjarvis.metadata.text;

import org.apache.spark.sql.types.*;
import uk.org.richardjarvis.utils.DataFrameUtils;
import uk.org.richardjarvis.utils.field.Recogniser;

import java.io.Serializable;
import java.util.List;

/**
 * The properties for a field including its meaning and name
 */
public class FieldProperties implements Serializable {

    private String name;
    private List<FieldMeaning> possibleMeanings = null;
    private boolean nullable = true;

    public List<FieldMeaning> getPossibleMeanings() {
        return possibleMeanings;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldProperties that = (FieldProperties) o;

        if (isNullable() != that.isNullable()) return false;
        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) return false;

        return getPossibleMeanings() != null ? getPossibleMeanings().containsAll(that.getPossibleMeanings()) : that.getPossibleMeanings() == null;

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

        Metadata metadata = DataFrameUtils.getMetadata(getMeaning().getMeaningType(), getMeaning().getFormat());
        return new StructField(getName(), type, isNullable(), metadata);

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
