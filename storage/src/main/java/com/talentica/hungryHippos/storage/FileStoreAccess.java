package com.talentica.hungryHippos.storage;

import com.talentica.hungryHippos.utility.marshaling.DataDescription;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by debasishc on 31/8/15.
 */
public class FileStoreAccess implements StoreAccess{
    List<RowProcessor> rowProcessors = new LinkedList<>();
    private int keyId;
    private int numFiles;
    private String base;
    private DataDescription dataDescription;

    public FileStoreAccess(String base, int keyId, int numFiles,
                           DataDescription dataDescription) {
        this.keyId = keyId;
        this.numFiles = numFiles;
        this.base = base;
        this.dataDescription = dataDescription;
    }

    @Override
    public void addRowProcessor(RowProcessor rowProcessor) {
        rowProcessors.add(rowProcessor);
    }

    @Override
    public void processRows() {
        int keyIdBit = 1 << keyId;
        for(int i=0;i<numFiles;i++){
            if((keyIdBit & i) > 0){
                processRows(i);
            }
        }
    }

    private void processRows(int fileId){
        try {
            DataInputStream in
                    = new DataInputStream(new FileInputStream(base + fileId));
            byte[] buf = new byte[dataDescription.getSize()];
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
            while(true){
                in.readFully(buf);
                for(RowProcessor p: rowProcessors){
                    p.processRow(byteBuffer);
                }
            }
        } catch (IOException e) {
            //e.printStackTrace();
        }
    }
}
