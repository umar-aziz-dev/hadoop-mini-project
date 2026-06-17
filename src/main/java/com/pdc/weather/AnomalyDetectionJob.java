package com.pdc.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class AnomalyDetectionJob {

    public static class AnomalyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private final Map<String, Stats> statsMap = new HashMap<>();
        private final Text outValue = new Text();
        private String targetYear = null;

        private static class Stats {
            final double mean;
            final double sigma;

            Stats(double mean, double sigma) {
                this.mean = mean;
                this.sigma = sigma;
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            targetYear = conf.get("target.year"); // May be null if not specified

            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                // Read the stats from the distributed cache file
                URI cacheUri = cacheFiles[0];
                FileSystem fs = FileSystem.get(cacheUri, conf);
                Path path = new Path(cacheUri);

                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            String station = parts[0].trim();
                            String[] statsParts = parts[1].split(",");
                            if (statsParts.length == 2) {
                                try {
                                    double mean = Double.parseDouble(statsParts[0]);
                                    double sigma = Double.parseDouble(statsParts[1]);
                                    statsMap.put(station, new Stats(mean, sigma));
                                } catch (NumberFormatException e) {
                                    // Ignore malformed numbers
                                }
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty() || line.startsWith("STATION")) {
                return; // Skip header/empty lines
            }

            String[] columns = StatsCalculationJob.parseCsvLine(line);
            if (columns.length > 6) {
                try {
                    String station = columns[0];
                    String date = columns[1];
                    String tempStr = columns[6];

                    // Optional: Filter by year (addressing the Cross-Group Portability Test)
                    if (targetYear != null && !targetYear.isEmpty()) {
                        // Date is typically in YYYY-MM-DD or YYYYMMDD format
                        if (!date.startsWith(targetYear)) {
                            return;
                        }
                    }

                    double temp = Double.parseDouble(tempStr);
                    if (temp == 9999.9) {
                        return;
                    }

                    Stats stats = statsMap.get(station);
                    if (stats != null && stats.sigma > 0.0) {
                        double deviation = Math.abs(temp - stats.mean);
                        double sigmaThreshold = 3.0 * stats.sigma;

                        if (deviation > sigmaThreshold) {
                            double sigmaMultiplier = deviation / stats.sigma;
                            // Format: STATION,DATE,TEMP,MEAN,SIGMA,DEVIATION_SIGMAS
                            String outputLine = String.format("%s,%s,%.2f,%.2f,%.2f,%.2fx",
                                    station, date, temp, stats.mean, stats.sigma, sigmaMultiplier);
                            outValue.set(outputLine);
                            context.write(NullWritable.get(), outValue);
                        }
                    }
                } catch (NumberFormatException e) {
                    // Ignore malformed rows
                }
            }
        }
    }
}
