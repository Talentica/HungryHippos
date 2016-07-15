/**
 * 
 */
package com.talentica.hungryHippos.coordination.exception;

/**
 * @author pooshans
 *
 */
public class UnsupportedTypeZkException extends Exception {
  private static final long serialVersionUID = 1L;
  private String name;

  public UnsupportedTypeZkException(String name) {
    super(name);
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

}
