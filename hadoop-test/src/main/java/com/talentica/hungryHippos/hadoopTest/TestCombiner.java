package com.talentica.hungryHippos.hadoopTest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Created by debasishc on 30/9/15.
 */
public class TestCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        BigDecimal sum1=new BigDecimal("0"), sum2=new BigDecimal("0");
        for(Text value:values){
            String [] parts = value.toString().split(",");
            sum1 = sum1.add(new BigDecimal(parts[0]));
            sum2 = sum2.add(new BigDecimal(parts[1]));
        }
        Text v1 = new Text(sum1.toString()+","+sum2.toString());
        context.write(key, v1);
    }
}