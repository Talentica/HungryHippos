/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryHippos.sharding.context;

import com.talentica.hungryhippos.filesystem.SerializableComparator;

import java.sql.Date;
import java.sql.Timestamp;

public enum ComparatorStore {
    INSTANCE;
    private SerializableComparator<String> stringSerializableComparator = (o1, o2) -> Integer.compare(o1.hashCode(),o2.hashCode());

    private SerializableComparator<Double> doubleSerializableComparator = (o1, o2) -> o1.compareTo(o2);

    private SerializableComparator<Float> floatSerializableComparator = (o1, o2) -> o1.compareTo(o2);

    private SerializableComparator<Timestamp> timestampSerializableComparator = (o1, o2) -> o1.compareTo(o2);

    private SerializableComparator<Long> longSerializableComparator = (o1, o2) -> o1.compareTo(o2);

    private SerializableComparator<Integer> integerSerializableComparator = (o1, o2) -> o1.compareTo(o2);

    private SerializableComparator<Date> dateSerializableComparator = (o1, o2) -> o1.compareTo(o2);

    private SerializableComparator<Short> shortSerializableComparator = (o1, o2) -> o1.compareTo(o2);

    private SerializableComparator<Byte> byteSerializableComparator = (o1, o2) -> o1.compareTo(o2);

    private SerializableComparator<Comparable> objectSerializableComparator = (o1,o2)-> o1.compareTo(o2);

    public SerializableComparator<? extends Comparable> getSerializableComparator(String dataType) {
        switch (dataType){
            case "STRING":
                return stringSerializableComparator;
            case "DOUBLE":
                return doubleSerializableComparator;
            case "FLOAT":
                return floatSerializableComparator;
            case "TIMESTAMP":
                return timestampSerializableComparator;
            case "LONG":
                return longSerializableComparator;
            case "INT":
                return integerSerializableComparator;
            case "DATE":
                return dateSerializableComparator;
            case "SHORT":
                return shortSerializableComparator;
            case "BYTE":
                return byteSerializableComparator;
        }
        return objectSerializableComparator;
    }
}
