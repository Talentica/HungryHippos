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

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryhippos.config.sharding.Column;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

public class FileStatistics implements Serializable {

    private static final long serialVersionUID = 4074885953480955556L;

    private ColumnStatistics[] columnStatistics;
    private transient List<BlockStatistics> blockStatisticsList;
    private transient ColumnStatistics[] currBlockColStatistics;
    private int blockRecCount = 0;
    private int maxRecordsPerBlock = 10000;
    private String[] columnNames;
    private long dataSize = 0L;
    private transient BlockStatistics blockStatistics;
    private long recLen = 0L;

    public FileStatistics(List<Column> columns, int cols,FieldTypeArrayDataDescription fieldTypeArrayDataDescription, long recLen, int maxRecordsPerBlock, SerializableComparator[] serializableComparators) {

        this.recLen = recLen;
        this.columnNames = new String[cols];
        this.maxRecordsPerBlock = maxRecordsPerBlock;
        this.columnStatistics = new ColumnStatistics[cols];
        for (int col = 0; col < cols; col++) {
            this.columnNames[col] = columns.get(col).getName();
            DataLocator dataLocator = fieldTypeArrayDataDescription.locateField(col);
            switch (dataLocator.getDataType()) {
                case STRING:
                    ColumnStatistics<String> stringColumnStatistics = new ColumnStatistics<>(serializableComparators[col]);
                    columnStatistics[col] = stringColumnStatistics;
                    break;
                case DOUBLE:
                    ColumnStatistics<Double> doubleColumnStatistics = new ColumnStatistics<>(serializableComparators[col]);
                    columnStatistics[col] = doubleColumnStatistics;
                    break;
                case INT:
                    ColumnStatistics<Integer> integerColumnStatistics = new ColumnStatistics<>(serializableComparators[col]);
                    columnStatistics[col] = integerColumnStatistics;
                    break;
                case LONG:
                    ColumnStatistics<Long> longColumnStatistics = new ColumnStatistics<>(serializableComparators[col]);
                    columnStatistics[col] = longColumnStatistics;
                    break;
                case DATE:
                    ColumnStatistics<Date> dateColumnStatistics = new ColumnStatistics<>(serializableComparators[col]);
                    columnStatistics[col] = dateColumnStatistics;
                    break;
                case FLOAT:
                    ColumnStatistics<Float> floatColumnStatistics = new ColumnStatistics<>(serializableComparators[col]);
                    columnStatistics[col] = floatColumnStatistics;
                    break;
                case SHORT:
                    ColumnStatistics<Short> shortColumnStatistics = new ColumnStatistics<>(serializableComparators[col]);
                    columnStatistics[col] = shortColumnStatistics;
                    break;
                case BYTE:
                    ColumnStatistics<Byte> byteColumnStatistics = new ColumnStatistics<>(serializableComparators[col]);
                    columnStatistics[col] = byteColumnStatistics;
                    break;
                case TIMESTAMP:
                    ColumnStatistics<Timestamp> characterColumnStatistics = new ColumnStatistics<>(serializableComparators[col]);
                    columnStatistics[col] = characterColumnStatistics;
                    break;
            }
        }

       blockRecCount = this.maxRecordsPerBlock;
    }

    public void setComparators(SerializableComparator[] comparators){
        for (int i = 0; i < comparators.length; i++) {
            columnStatistics[i].setComparator(comparators[i]);
        }
        if(blockStatistics!=null){
            blockStatistics.setComparators(comparators);
        }
    }

    public void updateColumnStatistics(int column, Object object) {

        if(column==0){
            if (blockRecCount == maxRecordsPerBlock) {
                blockRecCount = 0;
                blockStatistics = new BlockStatistics(dataSize,columnStatistics,recLen);
                currBlockColStatistics = blockStatistics.getColumnStatistics();
                blockStatisticsList.add(blockStatistics);
            }
            dataSize +=recLen;
            blockRecCount++;
        }
        currBlockColStatistics[column].setMinMax(object);
        if(blockRecCount == maxRecordsPerBlock&&column==columnNames.length-1){
            updateFileColumnStatistics();
        }
    }

    public void updateFileColumnStatistics() {
        if(blockStatistics!=null) {
            blockStatistics.setDataSize(blockRecCount * recLen);
            for (int column = 0; column < columnNames.length; column++) {
                columnStatistics[column].setMinMax(currBlockColStatistics[column].getMax());
                columnStatistics[column].setMinMax(currBlockColStatistics[column].getMin());
            }
        }
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

    public boolean isEmpty() {
        return dataSize ==0;
    }

    public List<BlockStatistics> getBlockStatisticsList() {
        return blockStatisticsList;
    }

    public void setBlockStatisticsList(List<BlockStatistics> blockStatisticsList) {
        this.blockStatisticsList = blockStatisticsList;
        if(dataSize>0){
            Iterator<BlockStatistics> iterator = blockStatisticsList.iterator();
            while(iterator.hasNext()){
                this.blockStatistics = iterator.next();
            }
            this.currBlockColStatistics = blockStatistics.getColumnStatistics();
        }
    }

    public long getDataSize() {
        return dataSize;
    }

    public ColumnStatistics[] getColumnStatistics() {
        return columnStatistics;
    }
}
