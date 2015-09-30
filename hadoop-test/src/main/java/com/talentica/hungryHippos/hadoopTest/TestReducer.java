package com.talentica.hungryHippos.hadoopTest;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by debasishc on 24/9/15.
 */
public class TestReducer extends Reducer<Text, Text, Text, Text> {
    //DescriptiveStatistics descriptiveStatistics1 = new DescriptiveStatistics();
    //DescriptiveStatistics descriptiveStatistics2 = new DescriptiveStatistics();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double sum1=0, sum2=0;
        for(Text value:values){
            String [] parts = value.toString().split(",");
            sum1+=(Double.parseDouble(parts[0]));
            sum2+=(Double.parseDouble(parts[1]));
        }
        Text v1 = new Text(""+sum1);
        Text v2 = new Text(""+sum2);
        context.write(key, v1);
        context.write(key, v2);
    }
}
