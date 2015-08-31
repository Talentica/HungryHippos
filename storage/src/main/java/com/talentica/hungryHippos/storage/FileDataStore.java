package com.talentica.hungryHippos.storage;

import java.io.*;

/**
 * Created by debasishc on 31/8/15.
 */
public class FileDataStore implements DataStore{
    private final int numFiles ;
    private NodeDataStoreIdCalculator nodeDataStoreIdCalculator;
    private ObjectOutputStream[] os;

    private String baseName = "data_";
    public FileDataStore(int numDimensions,
                         NodeDataStoreIdCalculator nodeDataStoreIdCalculator) throws IOException {
        this.numFiles = 1<<numDimensions;
        this.nodeDataStoreIdCalculator = nodeDataStoreIdCalculator;
        os = new ObjectOutputStream[numFiles];
        for(int i=0;i<numFiles;i++){
            os[i]
                    = new ObjectOutputStream(
                    new BufferedOutputStream(new FileOutputStream(baseName+i)));
        }
    }

    @Override
    public void storeRow(Object[] row) {
        int storeId = nodeDataStoreIdCalculator.storeId(row);
        try {
            os[storeId].writeObject(row);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public StoreAccess getStoreAccess(int keyId) {
        return new FileStoreAccess(baseName, keyId, numFiles);
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
