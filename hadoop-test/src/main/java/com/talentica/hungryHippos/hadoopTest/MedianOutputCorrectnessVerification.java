package com.talentica.hungryHippos.hadoopTest;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rajkishoreh on 24/10/16.
 */
public class MedianOutputCorrectnessVerification {

    public static void main(String[] args) throws IOException {

        validateArguments(args);
        String inputFilePath = args[0];
        String resultFilePath = args[1];

        BufferedReader bufferedReader = null;
        try {

            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFilePath)));
            String line;
            Map<String, DescriptiveStatistics[]> contextDescStats = new HashMap<>();
            while ((line = bufferedReader.readLine()) != null && !"".equals(line)) {
                processLine(line, contextDescStats);
            }
            bufferedReader.close();

            Map<String, String> expectedOutput = new HashMap<>();
            for (Map.Entry<String, DescriptiveStatistics[]> entry :
                    contextDescStats.entrySet()) {
                expectedOutput.put("3|" + entry.getKey(), entry.getValue()[0].getPercentile(50) + "");
                expectedOutput.put("4|" + entry.getKey(), entry.getValue()[1].getPercentile(50) + "");
            }

            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(resultFilePath)));

            while ((line = bufferedReader.readLine()) != null && !"".equals(line)) {
                String[] parts = line.toString().split("\t");
                validate(expectedOutput, parts);
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedReader != null)
                    bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void validate(Map<String, String> expectedOutput, String[] parts) {
        if (expectedOutput.containsKey(parts[0])) {
            if (!expectedOutput.get(parts[0]).equals(parts[1])) {
                throw new RuntimeException("For key:" + parts[0] + " Expected value : " + expectedOutput.get(parts[0]) + " is not matching with result file value :" + parts[1]);
            }
        } else {
            throw new RuntimeException(parts[0] + " key not found");
        }
    }

    private static void processLine(String line, Map<String, DescriptiveStatistics[]> contextDescStats) {
        String value = line;
        String[] parts = value.toString().split(",");
        double value1 = Double.parseDouble(parts[3]);
        double value2 = Double.parseDouble(parts[4]);
        int keyID = 0;
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[0]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[1]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[2]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[0]+"-"+parts[1]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[0]+"-"+parts[1]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[1]+"-"+parts[2]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[0]+"-"+parts[1]+"-"+parts[2]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[0]+"-"+parts[5]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[0]+"-"+parts[1]+"-"+parts[5]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[0]+"-"+parts[1]+"-"+parts[2]+"-"+parts[5]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[1]+"-"+parts[2]+"-"+parts[6]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[0]+"-"+parts[6]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[1]+"-"+parts[6]);
        addElement(value1,value2,contextDescStats,(keyID++)+"|"+parts[2]+"-"+parts[6]);
    }

    private static void validateArguments(String[] args) {
        if (args.length < 2) {
            throw new RuntimeException(
                    "Either missing 1st argument {input file path} or 2nd argument {result file path}");
        }
    }

    private static void addElement(double value1, double value2, Map<String, DescriptiveStatistics[]> contextDescStats, String keyText) {
        DescriptiveStatistics[] descriptiveStatisticsArr = contextDescStats.get(keyText);
        if (descriptiveStatisticsArr == null) {
            descriptiveStatisticsArr = new DescriptiveStatistics[2];
            descriptiveStatisticsArr[0] = new DescriptiveStatistics();
            descriptiveStatisticsArr[1] = new DescriptiveStatistics();
        }
        descriptiveStatisticsArr[0].addValue(value1);
        descriptiveStatisticsArr[1].addValue(value2);
        contextDescStats.put(keyText, descriptiveStatisticsArr);
    }
}
