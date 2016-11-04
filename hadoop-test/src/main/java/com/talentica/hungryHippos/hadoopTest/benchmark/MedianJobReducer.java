package com.talentica.hungryHippos.hadoopTest.benchmark;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by rajkishoreh on 24/10/16.
 */
public class MedianJobReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Text v1;
        Text v2;
        int size = 0;
        if (values instanceof Collection) {
            size = ((Collection<?>) values).size();
        }
        if (size == 1) {
            Text value = values.iterator().next();
            String[] parts = value.toString().split(",");
            v1 = new Text(parts[0]);
            v2 = new Text(parts[1]);
        }else{
            DescriptiveStatistics descriptiveStatistics1 = new DescriptiveStatistics();
            DescriptiveStatistics descriptiveStatistics2 = new DescriptiveStatistics();
            for(Text value:values){
                String [] parts = value.toString().split(",");
                descriptiveStatistics1.addValue(Double.parseDouble(parts[0]));
                descriptiveStatistics2.addValue(Double.parseDouble(parts[1]));
            }
            v1 = new Text(descriptiveStatistics1.getPercentile(50)+"");
            v2 = new Text(descriptiveStatistics2.getPercentile(50)+"");
        }
        String keyString = key.toString();
        context.write(new Text("3|"+keyString), v1);
        context.write(new Text("4|"+keyString), v2);
    }
}
