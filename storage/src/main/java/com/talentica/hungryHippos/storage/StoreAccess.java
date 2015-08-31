package com.talentica.hungryHippos.storage;

/**
 * Created by debasishc on 31/8/15.
 */
public interface StoreAccess {
    void addRowProcessor(RowProcessor rowProcessor);
    void processRows();

}
