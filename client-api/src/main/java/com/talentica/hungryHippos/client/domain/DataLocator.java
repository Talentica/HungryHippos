package com.talentica.hungryHippos.client.domain;

import java.io.Serializable;

/**
 * This class provides the description of the particular data.
 * 
 * @author debasishc.
 * @since 2015-09-01
 * @version 0.5.0
 */
public class DataLocator implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3265231048634949676L;

	public static enum DataType {
		BYTE, SHORT, INT, LONG, CHAR, FLOAT, DOUBLE, STRING
	}

	private int offset;
	private int size;
	private DataType dataType;

	/**
	 * Constructor of the DataLocator.
	 * 
	 * @param dataType
	 *            is the type of data such as string, int, long etc.
	 * @param offset
	 *            is the position of the data.
	 * @param size
	 *            is the size of the data.
	 */
	public DataLocator(DataType dataType, int offset, int size) {
		this.dataType = dataType;
		this.offset = offset;
		this.size = size;
	}

	/**
	 * To get the enumeration of the DataType.
	 * 
	 * @return DataType enumeration is type of data such as string, int etc.
	 */
	public DataType getDataType() {
		return dataType;
	}

	/**
	 * To get the offset of the data or value.
	 * 
	 * @return offset is the position of the data.
	 */
	public int getOffset() {
		return offset;
	}

	/**
	 * To get the size of the data.
	 * 
	 * @return size is the data type size.
	 */
	public int getSize() {
		return size;
	}
}
