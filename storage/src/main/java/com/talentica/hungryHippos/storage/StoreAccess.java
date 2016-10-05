package com.talentica.hungryHippos.storage;

/**
 * Created by debasishc on 31/8/15.
 */
public interface StoreAccess extends Iterable<DataFileAccess> {

  public void reset();

}
