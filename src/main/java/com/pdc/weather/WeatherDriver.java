package com.pdc.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Sets up Stage 1 job configuration
// Submits Stage 1 to Hadoop
// Waits for Stage 1 to finish
// Takes Stage 1 output, puts it in Distributed Cache
// Sets up Stage 2 job configuration
// Submits Stage 2 to Hadoop
// Waits for Stage 2 to finish
// Reports success or failure

public class WeatherDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WeatherDriver <input_path> <stage1_output_path> <stage2_output_path> [target_year]");
            return -1;
        }

        String inputPath = args[0];
        String stage1Out = args[1];
        String stage2Out = args[2];
        String targetYear = args.length >= 4 ? args[3] : null;

        Configuration conf = getConf();
        if (targetYear != null && !targetYear.trim().isEmpty()) {
            conf.set("target.year", targetYear.trim());
            System.out.println("Configuring Job 2 to filter anomalies for year: " + targetYear);
        }

        // ----------------------------------------------------
        // Stage 1: Stats Calculation (Mean & Standard Deviation)
        // ----------------------------------------------------
        System.out.println("Starting Stage 1: Stats Calculation...");
        Job job1 = Job.getInstance(conf, "Weather Anomaly Detection - Stage 1 (Stats)");
        job1.setJarByClass(WeatherDriver.class);

        job1.setMapperClass(StatsCalculationJob.StatsMapper.class);
        job1.setCombinerClass(StatsCalculationJob.StatsCombiner.class);
        job1.setReducerClass(StatsCalculationJob.StatsReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleSummaryWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        
        Path stage1OutPath = new Path(stage1Out);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(stage1OutPath)) {
            System.out.println("Deleting existing Stage 1 output directory: " + stage1Out);
            fs.delete(stage1OutPath, true);
        }
        FileOutputFormat.setOutputPath(job1, stage1OutPath);

        boolean job1Success = job1.waitForCompletion(true);
        if (!job1Success) {
            System.err.println("Stage 1 failed! Aborting pipeline.");
            return 1;
        }
        System.out.println("Stage 1 completed successfully.");

        // ----------------------------------------------------
        // Stage 2: Anomaly Filtering
        // ----------------------------------------------------
        System.out.println("Starting Stage 2: Anomaly Filtering...");
        Job job2 = Job.getInstance(conf, "Weather Anomaly Detection - Stage 2 (Filter)");
        job2.setJarByClass(WeatherDriver.class);

        job2.setMapperClass(AnomalyDetectionJob.AnomalyMapper.class);
        job2.setNumReduceTasks(0); // Map-only job

        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(inputPath));
        
        Path stage2OutPath = new Path(stage2Out);
        if (fs.exists(stage2OutPath)) {
            System.out.println("Deleting existing Stage 2 output directory: " + stage2Out);
            fs.delete(stage2OutPath, true);
        }
        FileOutputFormat.setOutputPath(job2, stage2OutPath);

        // Find the stats output file and add it to Distributed Cache
        FileStatus[] files = fs.listStatus(stage1OutPath);
        boolean cacheFileAdded = false;
        for (FileStatus file : files) {
            String fileName = file.getPath().getName();
            if (fileName.startsWith("part-r-") && file.getLen() > 0) {
                job2.addCacheFile(file.getPath().toUri());
                System.out.println("Added Stage 1 stats file to Distributed Cache: " + file.getPath().toString());
                cacheFileAdded = true;
            }
        }

        if (!cacheFileAdded) {
            System.err.println("Error: Stage 1 output (part-r-*) file not found. Cache file could not be added.");
            return 1;
        }

        boolean job2Success = job2.waitForCompletion(true);
        if (!job2Success) {
            System.err.println("Stage 2 failed!");
            return 1;
        }
        System.out.println("Stage 2 completed successfully. Anomaly detection pipeline finished.");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new WeatherDriver(), args);
        System.exit(exitCode);
    }
}
