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
