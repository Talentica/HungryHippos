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
package com.talentica.hungryHippos.sharding.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.utility.MapUtils;

/**
 * Sharding table printer utility to print sharding table for specified HH file on console.
 * 
 * @author nitink
 *
 */
public class ShardingTablePrinter {

  public static void main(String[] args) throws FileNotFoundException, JAXBException {
   args[0] = "/home/pooshans/hhuser/table";
    validateArguments(args);
    String shardingTablePath = args[0];
    ShardingApplicationContext context = new ShardingApplicationContext(shardingTablePath);
    Map<String, String> dataTypeMap = ShardingFileUtil.getDataTypeMap(context);
    System.out.println("###### Key to value to bucket number map ######");
    System.out
        .println(
            "\t" + "Key" + "\t" + "Value" + "\t" + "Bucket"
                + MapUtils.getFormattedString(ShardingFileUtil.readFromFileKeyToValueToBucket(
                    shardingTablePath + File.separatorChar + "keyToValueToBucketMap",
                    dataTypeMap)));
    System.out.println();
    System.out.println("###### Bucket to node numbers map ######");
    System.out.println("\t" + "Bucket" + "\t\t\t" + "Node"
        + MapUtils.getFormattedString(ShardingFileUtil.readFromFileBucketToNodeNumber(
            shardingTablePath + File.separatorChar + "bucketToNodeNumberMap")));
    System.out.println();
    
    System.out.println("###### Sharding split key map ######");
    System.out.println("\t" + "Key" + "\t\t\t" + "Value" + "\t\t\t" + "Split Count"
        + MapUtils.getFormattedString(ShardingFileUtil.readFromFileSplittedKeyValue(
            shardingTablePath + File.separatorChar + "splittedKeyValueMap")));
    System.out.println();
  }

  private static void validateArguments(String[] args) {
    if (args.length < 1) {
      System.err.println(
          "Please provide sharding-folder path parameters. e.g. java -cp sharding.jar com.talentica.hungryHippos.sharding.util.ShardingTablePrinter <sharding-folder>");
      System.exit(1);
    }
  }

}
