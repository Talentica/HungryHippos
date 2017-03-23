/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by debasishc on 24/9/15.
 */
public class TestMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String [] parts = value.toString().split(",");
        String value1 = parts[6];
        String value2 = parts[7];
        int keyID=0;
        for(int i=0;i<3;i++){
            Text keyText = new Text((keyID++)+"|"+parts[i]);
            Text valueText = new Text(value1+","+value2);
            context.write(keyText,valueText);
            for(int j=i+1;j<5;j++){
                keyText = new Text((keyID++)+"|"+parts[i]+"-"+parts[j]);
                valueText = new Text(value1+","+value2);
                context.write(keyText,valueText);
                for(int k=j+1;k<5;k++){
                    keyText = new Text((keyID++)+"|"+parts[i]+"-"+parts[j]+"-"+parts[k]);
                    valueText = new Text(value1+","+value2);
                    context.write(keyText,valueText);
                }
            }
        }
    }
}
