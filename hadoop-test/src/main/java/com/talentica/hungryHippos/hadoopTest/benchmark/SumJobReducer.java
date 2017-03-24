/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.hadoopTest.benchmark;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collection;


/**
 * Created by rajkishoreh on 25/10/16.
 */
public class SumJobReducer extends Reducer<Text, Text, Text, Text> {

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
        } else {
            long sum1 = 0L, sum2 = 0L;
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                sum1 += Long.parseLong(parts[0]);
                sum2 += Long.parseLong(parts[1]);
            }
            v1 = new Text(sum1+"");
            v2 = new Text(sum2+"");
        }
        String keyString = key.toString();
        context.write(new Text("3|"+keyString), v1);
        context.write(new Text("4|"+keyString), v2);
    }
}