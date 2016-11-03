package com.talentica.hungryHippos.storage;

/**
 * {@code RowProcessor} can be used by client to provide their own algorithm to process a row.
 */
public interface RowProcessor {

  /**
   * the method where the algorithm has to be written.
   */
  void process();
  
}
