package com.talentica.hungryHippos.storage;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
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

    public FileStoreAccess(String base, int keyId, int numFiles) {
        this.keyId = keyId;
        this.numFiles = numFiles;
        this.base = base;
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
            ObjectInputStream in
                    = new ObjectInputStream(new FileInputStream(base + fileId));
            while(true){
                Object[] row = (Object[])in.readObject();
                for(RowProcessor p: rowProcessors){
                    p.processRow(row);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
