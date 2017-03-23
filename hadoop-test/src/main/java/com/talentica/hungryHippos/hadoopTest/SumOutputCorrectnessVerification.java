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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rajkishoreh on 24/10/16.
 */
public class SumOutputCorrectnessVerification {

    public static void main(String[] args) {

        validateArguments(args);
        String inputFilePath = args[0];
        String resultFilePath = args[1];
        BufferedReader bufferedReader = null;
        try {

            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFilePath)));
            String line;
            Map<String, BigDecimal[]> context = new HashMap<>();
            while ((line = bufferedReader.readLine()) != null && !"".equals(line)) {
                processLine(line, context);
            }
            bufferedReader.close();

            Map<String, String> expectedOutput = new HashMap<>();
            for (Map.Entry<String, BigDecimal[]> entry :
                    context.entrySet()) {
                expectedOutput.put("3|" + entry.getKey(), entry.getValue()[0].toString());
                expectedOutput.put("4|" + entry.getKey(), entry.getValue()[1].toString());
            }

            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(resultFilePath)));

            while ((line = bufferedReader.readLine()) != null && !"".equals(line)) {
                validate(line, expectedOutput);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        finally {
            try{if(bufferedReader!=null)
                bufferedReader.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }

    }

    private static void validate(String line, Map<String, String> expectedOutput) {
        String[] parts = line.toString().split("\t");

        if (expectedOutput.containsKey(parts[0])) {
            double expectedValue = Double.parseDouble(expectedOutput.get(parts[0]));
            double resultValue = Double.parseDouble(parts[1]);
            double diff = Math.abs(resultValue - expectedValue);
            if (diff > .000000005) {
                throw new RuntimeException("For key:" + parts[0] + " Computed value : " + expectedOutput.get(parts[0]) + " is not matching with result file value :" + parts[1] + " Difference : " + diff);
            } else if (diff > 0) {
                System.out.println("For key:" + parts[0] + " Computed value : " + expectedOutput.get(parts[0]) + " is not matching with result file value :" + parts[1] + " Difference : " + diff);
            }
        } else {
            throw new RuntimeException(parts[0] + " key not found");
        }
    }

    private static void processLine(String line, Map<String, BigDecimal[]> context) {
        String value = line;
        String[] parts = value.toString().split(",");
        BigDecimal value1 = new BigDecimal(parts[3]);
        BigDecimal value2 = new BigDecimal(parts[4]);
        int keyID = 0;
        addElement(value1,value2,context,(keyID++)+"|"+parts[0]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[1]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[2]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[0]+"-"+parts[1]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[0]+"-"+parts[1]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[1]+"-"+parts[2]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[0]+"-"+parts[1]+"-"+parts[2]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[0]+"-"+parts[5]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[0]+"-"+parts[1]+"-"+parts[5]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[0]+"-"+parts[1]+"-"+parts[2]+"-"+parts[5]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[1]+"-"+parts[2]+"-"+parts[6]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[0]+"-"+parts[6]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[1]+"-"+parts[6]);
        addElement(value1,value2,context,(keyID++)+"|"+parts[2]+"-"+parts[6]);
    }

    private static void validateArguments(String[] args) {
        if (args.length < 2) {
            throw new RuntimeException(
                    "Either missing 1st argument {input file path} or 2nd argument {result file path}");
        }
    }

    private static void addElement(BigDecimal value1, BigDecimal value2, Map<String, BigDecimal[]> context, String keyText) {
        BigDecimal[] sumArray = context.get(keyText);
        if (sumArray == null) {
            sumArray = new BigDecimal[2];
            sumArray[0] = new BigDecimal("0");
            sumArray[1] = new BigDecimal("0");
        }
        sumArray[0] = sumArray[0].add(value1);
        sumArray[1] = sumArray[1].add(value2);

        context.put(keyText, sumArray);
    }
}
