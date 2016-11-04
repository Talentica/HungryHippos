package com.talentica.hungryHippos.hadoopTest.benchmark;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by rajkishoreh on 25/10/16.
 */
public class SumJobCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {


        int size = 0;

        if (values instanceof Collection) {
            size = ((Collection<?>) values).size();
        }
        if (size == 1) {
            context.write(key,values.iterator().next());
        } else {
            long sum1 = 0L, sum2 = 0L;
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                sum1 += Long.parseLong(parts[0]);
                sum2 += Long.parseLong(parts[1]);
            }
            Text v1 = new Text(sum1 + "," + sum2);
            context.write(key, v1);
        }
    }
}