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
import org.apache.orc.TypeDescription;

import java.sql.Date;
import java.sql.Timestamp;

import static org.apache.orc.TypeDescription.*;
import static org.apache.orc.TypeDescription.Category.*;

/**
 * Created by rajkishoreh on 7/5/18.
 */

public class OrcValueGetter {
    public Object getValue(ColumnVector columnVector, Category category, int rowNum) {
        switch (category) {
            case BYTE:
                BytesColumnVector bytesColumnVector = ((BytesColumnVector) columnVector);
                return bytesColumnVector.vector[rowNum][0];
            case INT:
                return (int) ((LongColumnVector) columnVector).vector[rowNum];
            case LONG:
                return ((LongColumnVector) columnVector).vector[rowNum];
            case SHORT:
                return (short) ((LongColumnVector) columnVector).vector[rowNum];
            case DATE:
                return new Date(DateWritable.daysToMillis((int)((LongColumnVector) columnVector).vector[rowNum]));
            case TIMESTAMP:
                return new Timestamp(((TimestampColumnVector) columnVector).time[rowNum]);
            case VARCHAR:
                return ((BytesColumnVector) columnVector).toString(rowNum);
            case STRING:
                return ((BytesColumnVector) columnVector).toString(rowNum);
            case DECIMAL:
                return ((DecimalColumnVector) columnVector).vector[rowNum];
            case FLOAT:
                return (float) ((DoubleColumnVector) columnVector).vector[rowNum];
            case DOUBLE:
                return ((DoubleColumnVector) columnVector).vector[rowNum];
        }
        return null;
    }

    public void read(VectorizedRowBatch batch, TypeDescription schema, int rowNum, Object[] objArray) {
        int i = 0;
        for (TypeDescription typeDescription : schema.getChildren()) {
            objArray[i] = getValue(batch.cols[i], typeDescription.getCategory(), rowNum);
            i++;
        }
    }

    public Object read(VectorizedRowBatch batch, TypeDescription schema, int rowNum, int colNum) {
        return getValue(batch.cols[colNum], schema.getChildren().get(colNum).getCategory(), rowNum);
    }
}
