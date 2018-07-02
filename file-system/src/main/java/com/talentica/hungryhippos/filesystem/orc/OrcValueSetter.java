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

package com.talentica.hungryhippos.filesystem.orc;

import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * Created by rajkishoreh on 7/5/18.
 */

public class OrcValueSetter {
    public void setValue(ColumnVector columnVector, Category category, int rowNum, Object value, int batchSize) {
        switch (category) {
            case BYTE:
                BytesColumnVector bytesColumnVector = ((BytesColumnVector) columnVector);
                bytesColumnVector.ensureSize(batchSize, false);
                bytesColumnVector.setVal(rowNum, new byte[]{(byte) value});
                break;
            case INT:
                ((LongColumnVector) columnVector).vector[rowNum] = (int) value;
                break;
            case LONG:
                ((LongColumnVector) columnVector).vector[rowNum] = (long) value;
                break;
            case SHORT:
                ((LongColumnVector) columnVector).vector[rowNum] = (short) value;
                break;
            case DATE:
                //((LongColumnVector) columnVector).vector[rowNum] = ((Date) value).getTime();
                ((LongColumnVector) columnVector).vector[rowNum] = DateWritable.dateToDays((Date) value);
                break;
            case TIMESTAMP:
                TimestampColumnVector timestampColumnVector = ((TimestampColumnVector) columnVector);
                timestampColumnVector.ensureSize(batchSize, false);
                timestampColumnVector.set(rowNum, (Timestamp) value);
                break;
            case VARCHAR:
                BytesColumnVector varcharColumnVector = ((BytesColumnVector) columnVector);
                varcharColumnVector.ensureSize(batchSize, false);
                varcharColumnVector.setVal(rowNum, ((String) value).getBytes());
                break;
            case STRING:
                BytesColumnVector stringColumnVector = ((BytesColumnVector) columnVector);
                stringColumnVector.ensureSize(batchSize, false);
                stringColumnVector.setVal(rowNum, ((String) value).getBytes());
                break;
            case DECIMAL:
                ((DecimalColumnVector) columnVector).vector[rowNum] = (HiveDecimalWritable) value;
                break;
            case FLOAT:
                ((DoubleColumnVector) columnVector).vector[rowNum] = (float) value;
                break;
            case DOUBLE:
                ((DoubleColumnVector) columnVector).vector[rowNum] = (double) value;
                break;
        }
    }

    public void addRow(VectorizedRowBatch batch,TypeDescription schema,Object[] objects){
        int row = batch.size++;
        int i =0;
        for(TypeDescription typeDescription:schema.getChildren()){
            setValue(batch.cols[i],typeDescription.getCategory(),row,objects[i],batch.getMaxSize());
            i++;
        }
    }
}
