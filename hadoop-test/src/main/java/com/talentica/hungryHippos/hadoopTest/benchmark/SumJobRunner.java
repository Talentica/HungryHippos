package com.talentica.hungryHippos.hadoopTest.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by rajkishoreh on 25/10/16.
 */
public class SumJobRunner extends Configured implements Tool{

    public static void main(String [] args) throws Exception{
        System.exit(ToolRunner.run(new SumJobRunner(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SumJobRunner.class);
        job.setMapperClass(SumJobMapper.class);
        job.setCombinerClass(SumJobCombiner.class);
        job.setReducerClass(SumJobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
