package com.talentica.hungryHippos.hadoopTest.benchmark;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by rajkishoreh on 25/10/16.
 */
public class SumJobMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String [] parts = value.toString().split(",");
        int keyID=0;
        Text valueText = new Text(parts[3]+","+parts[4]);
        context.write(new Text((keyID++)+"|"+parts[0]),valueText);
        context.write(new Text((keyID++)+"|"+parts[1]),valueText);
        context.write(new Text((keyID++)+"|"+parts[2]),valueText);
        context.write(new Text((keyID++)+"|"+parts[0]+"-"+parts[1]),valueText);
        context.write(new Text((keyID++)+"|"+parts[0]+"-"+parts[2]),valueText);
        context.write(new Text((keyID++)+"|"+parts[1]+"-"+parts[2]),valueText);
        context.write(new Text((keyID++)+"|"+parts[0]+"-"+parts[1]+"-"+parts[2]),valueText);
        context.write(new Text((keyID++)+"|"+parts[0]+"-"+parts[5]),valueText);
        context.write(new Text((keyID++)+"|"+parts[0]+"-"+parts[1]+"-"+parts[5]),valueText);
        context.write(new Text((keyID++)+"|"+parts[0]+"-"+parts[1]+"-"+parts[2]+"-"+parts[5]),valueText);
        context.write(new Text((keyID++)+"|"+parts[1]+"-"+parts[2]+"-"+parts[6]),valueText);
        context.write(new Text((keyID++)+"|"+parts[0]+"-"+parts[6]),valueText);
        context.write(new Text((keyID++)+"|"+parts[1]+"-"+parts[6]),valueText);
        context.write(new Text((keyID++)+"|"+parts[2]+"-"+parts[6]),valueText);

    }

}
