package com.talentica.hungryHippos.client.domain;

/**
 * Created by debasishc on 1/9/15.
 */
public interface DataDescription {
    public DataLocator locateField(int index);
    public int getSize();
    public String[] keyOrder();
	public int getNumberOfDataFields();
}