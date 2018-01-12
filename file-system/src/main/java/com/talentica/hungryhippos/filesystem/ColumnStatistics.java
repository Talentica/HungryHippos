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

package com.talentica.hungryhippos.filesystem;

import java.io.Serializable;

public class ColumnStatistics<T> implements Serializable, Cloneable{

    private static final long serialVersionUID = 4074885953480955556L;

    private T min;
    private T max;
    private transient SerializableComparator comparator;
    private boolean unset;

    public ColumnStatistics(SerializableComparator<T> comparator) {
        this.comparator = comparator;
        this.unset = true;
    }


    public void setMinMax(T val) {
        if(unset){
            this.max = val;
            this.min = val;
            unset = false;
            return;
        }
        if (comparator.compare(val, this.max) > 0) {
            this.max = val;
        }
        else if (comparator.compare(val, this.min) < 0) {
            this.min = val;
        }
    }

    public boolean equalTo(T val, SerializableComparator comparator) {
        return comparator.compare(val, this.max) <= 0 && comparator.compare(val, this.min) >= 0;
    }

    public boolean greaterThanEqualTo(T val, SerializableComparator comparator) {
        return  comparator.compare(this.max,val) >= 0;
    }

    public boolean greaterThan(T val, SerializableComparator comparator) {
        return comparator.compare(this.max, val) > 0;
    }

    public boolean lesserThanEqualTo(T val, SerializableComparator comparator) {
        return  comparator.compare(val, this.min) >= 0;
    }

    public boolean lesserThan(T val, SerializableComparator comparator) {
        return comparator.compare(val, this.min) > 0;
    }

    public boolean notEqualTo(T val, SerializableComparator comparator) {
        if(this.min==this.max){
            return comparator.compare(val,this.min)!=0;
        }
        return true;
    }


    @Override
    public String toString(){
        return "min :'"+this.min+"' max:'"+this.max+"'";
    }

    @Override
    public ColumnStatistics<T> clone() throws CloneNotSupportedException {
        ColumnStatistics<T> columnStatistics = new ColumnStatistics(this.comparator);
        return columnStatistics;
    }

    public T getMin() {
        return min;
    }

    public T getMax() {
        return max;
    }

    public void setComparator(SerializableComparator comparator) {
        this.comparator = comparator;
    }
}
