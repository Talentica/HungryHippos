package com.talentica.hungryHippos.storage;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Created by debasishc on 27/8/15.
 */
public interface DataStore {
    public void storeRow(ByteBuffer row, byte[] raw);

    /**
     * A moving stream.
     * @return
     */
    public StoreAccess getStoreAccess(int keyId);

    public void sync();
}
