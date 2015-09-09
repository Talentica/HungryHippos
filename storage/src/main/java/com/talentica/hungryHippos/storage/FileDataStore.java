package com.talentica.hungryHippos.storage;

import com.talentica.hungryHippos.utility.marshaling.DataDescription;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Created by debasishc on 31/8/15.
 */
public class FileDataStore implements DataStore{
    private final int numFiles ;
    private NodeDataStoreIdCalculator nodeDataStoreIdCalculator;
    private OutputStream[] os;
    private DataDescription dataDescription;

    private String baseName = "data_";
    public FileDataStore(int numDimensions,
                         NodeDataStoreIdCalculator nodeDataStoreIdCalculator,
                         DataDescription dataDescription) throws IOException {
        this(numDimensions,nodeDataStoreIdCalculator,dataDescription, false);
    }

    public FileDataStore(int numDimensions,
                         NodeDataStoreIdCalculator nodeDataStoreIdCalculator,
                         DataDescription dataDescription, boolean readOnly) throws IOException {
        this.numFiles = 1<<numDimensions;
        this.nodeDataStoreIdCalculator = nodeDataStoreIdCalculator;
        this.dataDescription = dataDescription;
        os = new OutputStream[numFiles];
        if(!readOnly) {
            for (int i = 0; i < numFiles; i++) {
                os[i] = new BufferedOutputStream(new FileOutputStream(baseName + i));
            }
        }
    }

    @Override
    public void storeRow(ByteBuffer row, byte[] raw) {
        int storeId = nodeDataStoreIdCalculator.storeId(row);
        try {
            os[storeId].write(raw);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public StoreAccess getStoreAccess(int keyId) {
        return new FileStoreAccess(baseName, keyId, numFiles,dataDescription);
    }

    @Override
    public void sync()  {
        for(int i=0;i<numFiles;i++){
            try {
                os[i].flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
