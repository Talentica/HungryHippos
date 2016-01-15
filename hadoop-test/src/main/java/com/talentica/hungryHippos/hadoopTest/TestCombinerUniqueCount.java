package com.talentica.hungryHippos.hadoopTest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by debasishc on 12/10/15.
 */
public class TestCombinerUniqueCount extends Reducer<Text, Text, Text, Text> {
    HashSet<String> firstHashSet = new HashSet<>();
    HashSet<String> secondHashSet = new HashSet<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            String[] firstParts = parts[0].split("|");
            String[] secondParts = parts[1].split("|");
            firstHashSet.addAll(Arrays.asList(firstParts));
            secondHashSet.addAll(Arrays.asList(secondParts));
        }

        StringBuilder firstPartString = new StringBuilder();
        for(String v: firstHashSet){
            firstPartString.append(v).append("|");
        }
        firstPartString.deleteCharAt(firstPartString.length()-1);

        StringBuilder secondPartString = new StringBuilder();
        for(String v: secondHashSet){
            secondPartString.append(v).append("|");
        }
        secondPartString.deleteCharAt(secondPartString.length()-1);

        context.write(key, new Text(firstPartString +","+secondPartString));

    }
}