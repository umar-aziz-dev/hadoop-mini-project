package com.pdc.weather;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom Writable class to store temperature aggregates for statistics calculation.
 * It holds:
 * - sum of temperatures
 * - sum of squared temperatures
 * - count of observations
 * 
 * This enables the use of a Combiner to merge aggregates before sending them to the Reducer.
 */
public class DoubleSummaryWritable implements Writable {
    private double sum;
    private double sumOfSquares;
    private long count;

    public DoubleSummaryWritable() {
        this.sum = 0.0;
        this.sumOfSquares = 0.0;
        this.count = 0L;
    }

    public DoubleSummaryWritable(double sum, double sumOfSquares, long count) {
        this.sum = sum;
        this.sumOfSquares = sumOfSquares;
        this.count = count;
    }

    public void addValue(double val) {
        this.sum += val;
        this.sumOfSquares += (val * val);
        this.count += 1L;
    }

    public void merge(DoubleSummaryWritable other) {
        this.sum += other.getSum();
        this.sumOfSquares += other.getSumOfSquares();
        this.count += other.getCount();
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public double getSumOfSquares() {
        return sumOfSquares;
    }

    public void setSumOfSquares(double sumOfSquares) {
        this.sumOfSquares = sumOfSquares;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeDouble(sumOfSquares);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readDouble();
        sumOfSquares = in.readDouble();
        count = in.readLong();
    }

    @Override
    public String toString() {
        return sum + "," + sumOfSquares + "," + count;
    }
}
