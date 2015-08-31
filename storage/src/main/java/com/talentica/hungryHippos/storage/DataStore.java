package com.talentica.hungryHippos.storage;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Created by debasishc on 27/8/15.
 */
public interface DataStore {
    public void storeRow(Object [] row);

    /**
     * A moving stream.
     * @return
     */
    public StoreAccess getStoreAccess(int keyId);

    public void sync();
}
