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
