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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Created by rajkishoreh on 7/5/18.
 */

public class OrcWriter implements Closeable{
    private Configuration conf;
    private TypeDescription schema;
    private Writer writer;
    private VectorizedRowBatch batch;
    private OrcValueSetter orcValueSetter;
    private boolean isClosed;
    private Path outputPath;

    public OrcWriter(TypeDescription schema, String[] columnNames, Path outputPath) throws IOException {
        this.conf = new Configuration();
        this.schema = schema;
        /*OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf);
        writerOptions.bloomFilterColumns(StringUtils.join(columnNames,","));
        writerOptions.version(OrcFile.Version.V_0_12);
        writerOptions.compress(CompressionKind.SNAPPY);
        writerOptions.encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION);
        writerOptions.setSchema(schema);*/
        this.writer = OrcFile.createWriter(outputPath,
                OrcFile.writerOptions(conf)
                        .setSchema(schema));
        this.batch = schema.createRowBatch();
        this.orcValueSetter = new OrcValueSetter();
        this.isClosed = false;
        this.outputPath = outputPath;
    }

    public OrcWriter(TypeDescription schema, String[] columnNames, Path outputPath , int batchSize) throws IOException {
        this.conf = new Configuration();
        this.schema = schema;
        /*OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf);
        writerOptions.bloomFilterColumns(StringUtils.join(columnNames,","));
        writerOptions.version(OrcFile.Version.V_0_12);
        writerOptions.compress(CompressionKind.SNAPPY);
        writerOptions.encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION);
        writerOptions.setSchema(schema);*/
        this.writer = OrcFile.createWriter(outputPath,
                OrcFile.writerOptions(conf)
                        .setSchema(schema));
        this.batch = schema.createRowBatch(batchSize);
        this.orcValueSetter = new OrcValueSetter();
        this.isClosed = false;
        this.outputPath = outputPath;
    }

    public void write(Object[] objects) throws IOException {
        if(isClosed){
            throw new IOException("OrcWriter is already closed");
        }
        orcValueSetter.addRow(batch,schema,objects);
        if (batch.size == batch.getMaxSize()) {
            writer.addRowBatch(batch);
            batch.reset();
        }
    }

    public void flush() throws IOException {
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
    }


    @Override
    public void close() throws IOException {
        if(!isClosed){
            writer.close();
            isClosed = true;
            String fileName = outputPath.getName();
            new File(outputPath.getParent().toString()+File.separator+"."+fileName+".crc").delete();
        }
    }
}
