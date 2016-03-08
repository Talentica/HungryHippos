package com.talentica.hungryHippos.storage;

import java.nio.ByteBuffer;

/**
 * Created by debasishc on 27/8/15.
 */
public interface DataStore {
	public void storeRow(int storeId, ByteBuffer row, byte[] raw);

    /**
     * A moving stream.
     * @return
     */
    public StoreAccess getStoreAccess(int keyId);

    public void sync();
}
