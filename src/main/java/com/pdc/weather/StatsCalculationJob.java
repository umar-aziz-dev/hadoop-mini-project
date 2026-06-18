package com.pdc.weather;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StatsCalculationJob {

    // StatsMapper => 
        // Reads one CSV row
        // Extracts Station ID (column 0) and Temperature (column 6)
        // Skips header rows and missing values (9999.9 = NOAA code for "no reading")
        // Emits: StationID → {sum=temp, sumOfSquares=temp², count=1}

    public static class StatsMapper extends Mapper<LongWritable, Text, Text, DoubleSummaryWritable> {
        private final Text stationKey = new Text();
        private final DoubleSummaryWritable outValue = new DoubleSummaryWritable(); // Custom writable to hold sum, sum of squares, and count

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty() || line.startsWith("STATION")) {
                // Skip header or empty lines
                return;
            }

            String[] columns = parseCsvLine(line);
            if (columns.length > 6) {
                try {
                    String station = columns[0];
                    String tempStr = columns[6];
                    
                    double temp = Double.parseDouble(tempStr);
                    // 9999.9 is the standard GSOD missing value indicator for temperature
                    if (temp == 9999.9) {
                        return;
                    }

                    stationKey.set(station);
                    outValue.setSum(temp);
                    outValue.setSumOfSquares(temp * temp);
                    outValue.setCount(1L); // 1L => count of one observation

                    context.write(stationKey, outValue);
                } catch (NumberFormatException e) {
                    // Ignore malformed rows
                }
            }
        }
    }

    // StatsCombiner =>
        // Runs locally on each machine before data is sent over the network
        // Pre-adds partial sums together
        // Reduces network traffic dramatically
        // Think of it as a "local mini-reducer"
    public static class StatsCombiner extends Reducer<Text, DoubleSummaryWritable, Text, DoubleSummaryWritable> {
        private final DoubleSummaryWritable combinedValue = new DoubleSummaryWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleSummaryWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            double sumOfSquares = 0.0;
            long count = 0L;

            for (DoubleSummaryWritable val : values) {
                sum += val.getSum();
                sumOfSquares += val.getSumOfSquares();
                count += val.getCount();
            }

            combinedValue.setSum(sum);
            combinedValue.setSumOfSquares(sumOfSquares);
            combinedValue.setCount(count);

            context.write(key, combinedValue);
        }
    }

    // StatsReducer =>
        // Receives all partial sums for one station from ALL machines
        // Adds them all up
        // Applies the formula → computes mean and sigma
        // Writes: 72530094846\t49.93,21.76

    public static class StatsReducer extends Reducer<Text, DoubleSummaryWritable, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<DoubleSummaryWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            double sumOfSquares = 0.0;
            long count = 0L;

            for (DoubleSummaryWritable val : values) {
                sum += val.getSum();
                sumOfSquares += val.getSumOfSquares();
                count += val.getCount();
            }

            if (count > 1) {
                double mean = sum / count;
                double variance = (sumOfSquares / count) - (mean * mean);
                // Handle small precision issues that could make variance negative
                double sigma = Math.sqrt(Math.max(0.0, variance));

                // Output format: station \t mean,sigma
                outValue.set(mean + "," + sigma);
                context.write(key, outValue);
            }
        }
    }

    /**
     * Parse CSV lines supporting double quotes.
     */
    public static String[] parseCsvLine(String line) {
        List<String> tokens = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                tokens.add(sb.toString().trim());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        tokens.add(sb.toString().trim());
        return tokens.toArray(new String[0]);
    }
}
