package com.talentica.hungryHippos.node.datareceiver;

/**
 * Created by rajkishoreh on 18/5/17.
 */
public class Calculator {

    public static int indexCalculator(int[] values, int base) {
        int index = 0;
        for (int i = 0; i < values.length; i++) {
            index = index + values[i] * power(base, i);
        }
        return index;
    }

    public static int power(int x, int pow) {
        int value = 1;
        for (int i = 0; i < pow; i++) {
            value = value * x;
        }
        return value;
    }

    public static String combinationKeyGenerator(int[] values){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(values[0]);
        for (int i = 1; i < values.length; i++) {
            stringBuilder.append("_").append(values[i]);
        }
        return stringBuilder.toString();
    }
}
