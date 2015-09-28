package com.talentica.hungryHippos.hadoopTest;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by debasishc on 24/9/15.
 */
public class TestReducer extends Reducer<Text, Text, Text, Text> {
    DescriptiveStatistics descriptiveStatistics1 = new DescriptiveStatistics();
    DescriptiveStatistics descriptiveStatistics2 = new DescriptiveStatistics();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for(Text value:values){
            String [] parts = value.toString().split(",");
            descriptiveStatistics1.addValue(Double.parseDouble(parts[0]));
            descriptiveStatistics2.addValue(Double.parseDouble(parts[1]));
        }
        Text v1 = new Text(""+descriptiveStatistics1.getPercentile(50));
        Text v2 = new Text(""+descriptiveStatistics2.getPercentile(50));
        context.write(key, v1);
        context.write(key, v2);
        descriptiveStatistics1.clear();
        descriptiveStatistics2.clear();
    }
}
