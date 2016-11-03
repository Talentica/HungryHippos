package com.talentica.hungryHippos.client.domain;

import java.util.HashMap;
import java.util.Map;

/**
 * {@code MutableCharArrayStringCache} is used for storing and retrieving MutableCharArrayString
 * which was created by the system.
 * 
 * @author debashishc
 *
 */
public final class MutableCharArrayStringCache {

  private final Map<Integer, MutableCharArrayString> stringLengthToMutableCharArrayStringCache =
      new HashMap<>();

  private final Map<String, MutableCharArrayString> mutableCharArrayStringCache =
      new HashMap<>(10000);

  private MutableCharArrayStringCache() {}

  public MutableCharArrayString getMutableStringFromCacheOfSize(int size) {
    MutableCharArrayString charArrayString = stringLengthToMutableCharArrayStringCache.get(size);
    if (charArrayString == null) {
      charArrayString = new MutableCharArrayString(size);
      stringLengthToMutableCharArrayStringCache.put(size, charArrayString);

    } else {
      charArrayString.reset();
    }
    return charArrayString;
  }

  public MutableCharArrayString getMutableStringFromCacheOfSize(String alreadyPresent) {
    MutableCharArrayString charArrayString = mutableCharArrayStringCache.get(alreadyPresent);
    return charArrayString;
  }

  public void add(String alreadyPresent, MutableCharArrayString charArrayString) {

    mutableCharArrayStringCache.put(alreadyPresent, charArrayString);

  }

  /**
   * creates a new MutableCharArrayStringCache.
   * 
   * @return a new instance of MutableCharArrayStringCache.
   */
  public static MutableCharArrayStringCache newInstance() {
    return new MutableCharArrayStringCache();
  }

}
