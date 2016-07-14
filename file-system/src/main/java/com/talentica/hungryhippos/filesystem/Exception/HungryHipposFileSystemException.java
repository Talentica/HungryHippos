package com.talentica.hungryhippos.filesystem.Exception;

/**
 * Exception Class used on FileSystem.
 * 
 * @author sudarshans
 *
 */
public class HungryHipposFileSystemException extends RuntimeException {

	/**
	 * Serial UID for the serialization
	 */
	private static final long serialVersionUID = 2529936268877346952L;

	private String message = null;

	public HungryHipposFileSystemException() {
		super();
	}

	public HungryHipposFileSystemException(String message) {
		super(message);
		this.message = message;
	}

	public HungryHipposFileSystemException(String message, Throwable cause) {
		super(message, cause);
		this.message = message;
	}

	@Override
	public String toString() {
		return message;
	}

}
