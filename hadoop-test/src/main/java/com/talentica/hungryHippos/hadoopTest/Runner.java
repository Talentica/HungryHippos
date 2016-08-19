package com.talentica.hungryHippos.hadoopTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by debasishc on 24/9/15.
 */
public class Runner {
    public static void main(String [] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "metric");
        job.setNumReduceTasks(10);
        job.setJarByClass(Runner.class);
        job.setMapperClass(TestMapper.class);
        //job.setCombinerClass(TestCombiner.class);
        job.setReducerClass(TestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
