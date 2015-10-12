package com.talentica.hungryHippos.hadoopTest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by debasishc on 12/10/15.
 */
public class TestMapperUniqueCount extends Mapper<Object, Text, Text, Text>{
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String [] parts = value.toString().split(",");
        String value1 = (parts[6]);
        String value2 =(parts[7]);
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
