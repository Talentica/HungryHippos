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
        this.numFiles = 1<<numDimensions;
        this.nodeDataStoreIdCalculator = nodeDataStoreIdCalculator;
        this.dataDescription = dataDescription;
        os = new OutputStream[numFiles];

        for(int i=0;i<numFiles;i++){
            os[i] = new BufferedOutputStream(new FileOutputStream(baseName+i));
        }
    }

    @Override
    public void storeRow(byte[] row) {
        int storeId = nodeDataStoreIdCalculator.storeId(ByteBuffer.wrap(row));
        try {
            os[storeId].write(row);
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
