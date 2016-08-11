package uk.org.richardjarvis.utils.image;

/**
 * Created by rjarvis on 21/04/16.
 */
public class Colour implements Comparable<Colour> {

    private static final double HUE_IMPORTANCE = 47.5;
    private static final double SATURATION_IMPORTANCE = 28.75;
    private static final double LUMINANCE_IMPORTANCE = 23.75;
    private String name;
    private double hue;
    private double saturation;
    private double luminance;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getHue() {
        return hue;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Colour{");
        sb.append("name='").append(name).append('\'');
        sb.append(", hue=").append(hue);
        sb.append(", saturation=").append(saturation);
        sb.append(", luminance=").append(luminance);
        sb.append('}');
        return sb.toString();
    }

    public void setHue(double hue) {
        this.hue = hue;
    }

    public double getSaturation() {
        return saturation;
    }

    public void setSaturation(double saturation) {
        this.saturation = saturation;
    }

    public double getLuminance() {
        return luminance;
    }

    public void setLuminance(double luminance) {
        this.luminance = luminance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Colour colour = (Colour) o;

        if (Double.compare(colour.getHue(), getHue()) != 0) return false;
        if (Double.compare(colour.getSaturation(), getSaturation()) != 0) return false;
        if (Double.compare(colour.getLuminance(), getLuminance()) != 0) return false;
        return getName() != null ? getName().equals(colour.getName()) : colour.getName() == null;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = getName() != null ? getName().hashCode() : 0;
        temp = Double.doubleToLongBits(getHue());
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(getSaturation());
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(getLuminance());
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public Colour(String name, double hue, double saturation, double luminance) {
        this.name = name;
        this.hue = hue;
        this.saturation = saturation;
        this.luminance = luminance;
    }

    @Override
    public int compareTo(Colour colour) {
        double distance = (getHue() - colour.getHue()) * HUE_IMPORTANCE +
                (getSaturation() - colour.getSaturation()) * SATURATION_IMPORTANCE +
                (getLuminance() - colour.getLuminance()) * LUMINANCE_IMPORTANCE;
        //int answer = ((getHue() > colour.getHue()) ? 1 : -1) * (int) Math.ceil(distance*10000);
        int answer = (int) Math.ceil(distance*10000);
     //   System.out.println(toString() +" =?= " + colour.toString() + ": " + answer);
        return answer;
    }
}
