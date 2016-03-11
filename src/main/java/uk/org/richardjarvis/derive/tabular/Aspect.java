package uk.org.richardjarvis.derive.tabular;

import java.io.Serializable;

public class Aspect implements Serializable {
    private String eventType;
    private String window;
    private String valueName;
    private CubeType cubeType;
    public enum CubeType {MEAN, SUM, OTHER, COUNT}
    public String getEventType() {
        return eventType;
    }

    public String getWindow() {
        return window;
    }

    public String getValueName() {
        return valueName;
    }

    public CubeType getCubeType() {
        return cubeType;
    }

    private final String NULL_NAME = "ALL";
    private final String FIELD_SEPARATOR = "_";

    public String toColumnName() {
        StringBuilder sb = new StringBuilder();
        appendSafeNull(sb, eventType);
        sb.append(FIELD_SEPARATOR);
        appendSafeNull(sb, window);
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        appendSafeNull(sb, eventType);
        sb.append(FIELD_SEPARATOR);
        appendSafeNull(sb, window);
        sb.append(FIELD_SEPARATOR);
        appendSafeNull(sb, valueName);
        sb.append(FIELD_SEPARATOR);
        appendSafeNull(sb, cubeType);

        return sb.toString();

    }


    private void appendSafeNull(StringBuilder sb, Object string) {
        if (string == null) {
            sb.append(NULL_NAME);
        } else {
            sb.append(string.toString());
        }

    }

    public Aspect(String eventType, String window, String valuName, CubeType cubeType) {
        this.eventType = eventType;
        this.window = window;
        this.valueName = valuName;
        this.cubeType = cubeType;
    }

    public Aspect(Aspect source) {
        this.eventType = source.eventType;
        this.window = source.window;
        this.valueName = source.valueName;
        this.cubeType = CubeType.valueOf(source.cubeType.toString());
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((eventType == null) ? 0 : eventType
                .hashCode());
        result = prime
                * result
                + ((window == null) ? 0 : window
                .hashCode());
        result = prime
                * result
                + ((valueName == null) ? 0 : valueName
                .hashCode());
        result = prime
                * result
                + ((cubeType == null) ? 0 : cubeType
                .hashCode());


        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Aspect))
            return false;
        Aspect objA = (Aspect) obj;

        return doesMatch(getCubeType(), objA.getCubeType()) && doesMatch(getEventType(),objA.getEventType()) && doesMatch(getValueName(),objA.getValueName()) && doesMatch(getWindow(),objA.getWindow());
    }

    private boolean doesMatch(Object  a, Object b) {
        return (a == null || b == null || a.equals(b) );
    }
}
