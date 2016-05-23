package com.talentica.test.youtube.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopVideosHadoopJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "YoutubeTopVideosTestJob");
	    job.setJarByClass(getClass());
	    job.setJobName(getClass().getSimpleName());
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(YoutubeTopVideosMapper.class);
		job.setReducerClass(YoutubeTopVideosReducer.class);
		job.setCombinerClass(YoutubeTopVideosCombiner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int rc = ToolRunner.run(new TopVideosHadoopJob(), args);
		System.exit(rc);
	}

}