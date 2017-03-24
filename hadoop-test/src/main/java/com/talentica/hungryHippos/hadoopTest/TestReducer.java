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
package com.talentica.hungryHippos.hadoopTest;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by debasishc on 24/9/15.
 */
public class TestReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        BigDecimal sum1=new BigDecimal("0"), sum2=new BigDecimal("0");
        for(Text value:values){
            String [] parts = value.toString().split(",");
            sum1 = sum1.add(new BigDecimal(parts[0]));
            sum2 = sum2.add(new BigDecimal(parts[1]));
        }
        String keyString = key.toString();
        Text v1 = new Text(sum1.toString());
        context.write(new Text("6|"+keyString), v1);
        Text v2 = new Text(sum2.toString());
        context.write(new Text("7|"+keyString), v2);
    }
}
