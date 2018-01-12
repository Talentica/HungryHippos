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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class BlockStatistics implements Serializable{

    private static final long serialVersionUID = 4074885953480955556L;

    private ColumnStatistics[] columnStatistics;
    private long startPos;
    private long dataSize;
    private long recLen;


    public BlockStatistics(long startPos, ColumnStatistics[] columnStatistics, long recLen) {
        this.startPos = startPos;
        this.recLen =recLen;
        this.columnStatistics = new ColumnStatistics[columnStatistics.length];
        for (int col = 0; col < columnStatistics.length; col++) {
            try {
                this.columnStatistics[col] = columnStatistics[col].clone();
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }
    }

    public ColumnStatistics[] getColumnStatistics() {
        return columnStatistics;
    }

    public boolean equalTo(Integer col, Object value, ArrayList<SerializableComparator> comparators) {
        return columnStatistics[col].equalTo(value,comparators.get(col));
    }

    public boolean greaterThan(Integer col, Object value, ArrayList<SerializableComparator> comparators) {
        return columnStatistics[col].greaterThan(value,comparators.get(col));
    }

    public boolean greaterThanEqualTo(Integer col, Object value, ArrayList<SerializableComparator> comparators) {
        return columnStatistics[col].greaterThanEqualTo(value,comparators.get(col));
    }

    public boolean lesserThan(Integer col, Object value, ArrayList<SerializableComparator> comparators) {
        return columnStatistics[col].lesserThan(value,comparators.get(col));
    }

    public boolean lesserThanEqualTo(Integer col, Object value, ArrayList<SerializableComparator> comparators) {
        return columnStatistics[col].lesserThanEqualTo(value,comparators.get(col));
    }

    public boolean notEqualTo(Integer col, Object value, ArrayList<SerializableComparator> comparators) {
        return columnStatistics[col].notEqualTo(value,comparators.get(col));
    }

    @Override
    public String toString() {
        return Arrays.toString(columnStatistics);
    }


    public void incrementDataSize(){
        dataSize +=recLen;
    }

    public void setDataSize(long dataSize){
        this.dataSize =dataSize;
    }

    public long getStartPos() {
        return startPos;
    }

    public long getDataSize() {
        return dataSize;
    }


    public void setComparators(SerializableComparator[] comparators){
        for (int i = 0; i < comparators.length; i++) {
            columnStatistics[i].setComparator(comparators[i]);
        }
    }

    public void setComparatorsArray(ArrayList<SerializableComparator> comparators){
        for (int i = 0; i < comparators.size(); i++) {
            columnStatistics[i].setComparator(comparators.get(i));
        }
    }
}
