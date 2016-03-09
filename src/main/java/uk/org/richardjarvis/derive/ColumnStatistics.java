package uk.org.richardjarvis.derive;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rjarvis on 03/03/16.
 */
public class ColumnStatistics implements Serializable {

    Double sum = 0.0;
    Long notNullCount = 0l;
    Double mSquared = 0.0;
    Double mean = 0.0;
    Long count = 0l;
    Map<String, Long> frequencyTable = new HashMap<>();

    public ColumnStatistics(Double sum, Long notNullCount, Double mSquared) {
        this.sum = sum;
        this.notNullCount = notNullCount;
        this.mSquared = mSquared;
        this.mean = sum / notNullCount;
    }

    public ColumnStatistics() {

    }

    public Long getCount() {
        return count;
    }

    public void addValue(Double value) {
        count++;

        if (value != null) {
            notNullCount++;
            Double delta = value - mean;
            mean += delta / notNullCount;
            mSquared += delta * (value - mean);
            sum += value;
        }
    }

    public Double getStandardDeviation() {
        return Math.sqrt(mSquared / (notNullCount - 1));
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public Long getNotNullCount() {
        return notNullCount;
    }

    public void setNotNullCount(Long notNullCount) {
        this.notNullCount = notNullCount;
    }

    public Double getmSquared() {
        return mSquared;
    }

    public void setmSquared(Double mSquared) {
        this.mSquared = mSquared;
    }

    public Double getMean() {
        return mean;
    }

    public ColumnStatistics combine(ColumnStatistics statistics) {
        // from Chan et al.
        if (statistics != null) {
            Double delta = statistics.getMean() - this.getMean();
            Double mSq = getmSquared() + statistics.getmSquared() + (delta * delta) * (double) (this.getNotNullCount() * statistics.getNotNullCount()) / (this.getNotNullCount() + statistics.getNotNullCount());
            return new ColumnStatistics(this.getSum() + statistics.getSum(), this.getNotNullCount() + statistics.getNotNullCount(), mSq);
        } else {
            return null;
        }
    }

    public void addToFrequnecyTable(String value, Long count) {
        frequencyTable.put(value, count);
    }

    public Double getFrequency(String value) {

        Long freq = frequencyTable.get(value);
        if (freq!=null) {
            return (double)freq / getCount();
        } else {
            return null;
        }
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ColumnStatistics{");
        sb.append("sum=").append(sum);
        sb.append(", notNullCount=").append(notNullCount);
        sb.append(", standard deviation=").append(getStandardDeviation());
        sb.append(", mean=").append(mean);
        sb.append('}');
        return sb.toString();
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
