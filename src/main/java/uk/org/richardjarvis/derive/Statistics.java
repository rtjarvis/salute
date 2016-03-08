package uk.org.richardjarvis.derive;

import java.io.Serializable;

/**
 * Created by rjarvis on 03/03/16.
 */
public class Statistics implements Serializable {

    Double sum = 0.0;
    Long count = 0l;
    Double mSquared = 0.0;
    Double mean = 0.0;

    public Statistics(Double sum, Long count, Double mSquared) {
        this.sum = sum;
        this.count = count;
        this.mSquared = mSquared;
        this.mean = sum / count;
    }

    public Statistics() {

    }

    public void addValue(Double value) {
        if (value != null) {
            count++;
            Double delta = value - mean;
            mean += delta / count;
            mSquared += delta * (value - mean);
            sum += value;
        }
    }

//    private void addValues(Double sum, Long count, Double mSquared) {
//        // from Chan et al.
//
//        this.count += count;
//        Double delta = sum / count - mean;
//        mean += delta * count / this.count;
//        this.mSquared += mSquared;
//        this.sum+=sum;
//    }

    public Double getStandardDeviation() {
        return Math.sqrt(mSquared / (count - 1));
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Double getmSquared() {
        return mSquared;
    }

    public Double getMean() {
        return mean;
    }

    public void setmSquared(Double mSquared) {
        this.mSquared = mSquared;
    }

    public Statistics combine(Statistics statistics) {
        // from Chan et al.
        if (statistics != null) {
            Double delta = statistics.getMean() - this.getMean();
            Double mSq = getmSquared() + statistics.getmSquared() + (delta * delta) * (double) (this.getCount() * statistics.getCount()) / (this.getCount() + statistics.getCount());
            return new Statistics(this.getSum() + statistics.getSum(), this.getCount() + statistics.getCount(), mSq);
        } else {
            return null;
        }

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Statistics{");
        sb.append("sum=").append(sum);
        sb.append(", count=").append(count);
        sb.append(", standard deviation=").append(getStandardDeviation());
        sb.append(", mean=").append(mean);
        sb.append('}');
        return sb.toString();
    }

}
