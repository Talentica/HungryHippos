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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by rajkishoreh on 7/5/18.
 */

public class OrcReader implements Closeable{
    private Reader reader;
    private TypeDescription schema;
    private VectorizedRowBatch batch;
    private OrcValueGetter orcValueGetter;
    private int numCols ;
    private RecordReader recordReader;
    private int idx;
    private boolean isClosed;
    private int[] cols;
    private String[] columnNames;
    private Object[] completeRow;
    private Object[] partialRow;
    private int numberOfStripes;

    public OrcReader(Configuration configuration, Path inputPath, Reader.Options options) throws IOException {
        this(configuration,inputPath,options,null);
    }

    public OrcReader(Configuration configuration, Path inputPath, Reader.Options options, int[] cols) throws IOException {
        this.reader = OrcFile.createReader(inputPath,
                OrcFile.readerOptions(configuration));
        this.schema = reader.getSchema();
        this.columnNames = reader.options().getColumnNames();
        this.batch = schema.createRowBatch();
        if(options==null){
            this.recordReader = reader.rows();
        }else{
            this.recordReader = reader.rows(options);
        }
        this.orcValueGetter = new OrcValueGetter();
        this.numCols = schema.getChildren().size();
        this.idx=batch.size-1;
        this.isClosed = false;
        if(cols!=null){
            this.cols = cols;
            this.partialRow = new Object[cols.length];
        }
        this.completeRow = new Object[numCols];
        this.numberOfStripes = this.reader.getStripes().size();
    }

   public boolean hasNext() throws IOException {
        if(isClosed){
            throw new IOException("OrcReader is already closed");
        }
        idx++;
        if(idx<batch.size) return true;
        if(recordReader.nextBatch(batch)){
            idx=0;
            return true;
       }
       return false;
   }

   public Object[] nextCompleteRow(){
        orcValueGetter.read(batch,schema,idx,completeRow);
        return completeRow;
   }

    public Object[] nextPartialRow(){
        for (int index = 0; index < cols.length; index++) {
            partialRow[index]=orcValueGetter.getValue(batch.cols[cols[index]],schema.getChildren().get(cols[index]).getCategory(), this.idx);
        }
        return partialRow;
    }

    @Override
    public void close() throws IOException {

       if(!isClosed){
           recordReader.close();
           isClosed = true;
       }
    }

    public TypeDescription getSchema() {
        return schema;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public int getNumberOfStripes() {
        return numberOfStripes;
    }

    public Reader getReader() {
        return reader;
    }
}
