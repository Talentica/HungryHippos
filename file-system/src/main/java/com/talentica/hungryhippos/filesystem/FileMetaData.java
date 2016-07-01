package com.talentica.hungryhippos.filesystem;

import java.io.Serializable;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public class FileMetaData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@Nonnull
	private String fileName;
	@Nonnull
	private String type;
	@Nonnegative
	private long size;
	private boolean sharded;
	private boolean replicated;
	private int replicationNumber;
	private String[] replicatedFiles;

	/**
	 * FileMetaData Constructor
	 * 
	 * @param fileName
	 *            :- name of the file
	 * @param type
	 *            :- file-type. i.e; .csv or .txt .
	 * @param size
	 *            :- file size in bytes.
	 */
	public FileMetaData(String fileName, String type, long size) {
		this.fileName = fileName;
		this.type = type;
		this.size = size;
	}

	/**
	 * Method returns the name of the file associated with the object.
	 * 
	 * @return fileName
	 */
	public String getFileName() {
		return fileName;
	}

	/**
	 * Method returns what type of file i.e; .txt or .csv
	 * 
	 * @return type
	 */
	public String getType() {
		return type;
	}

	/**
	 * Methods retrieves the size of the file.
	 * 
	 * @return
	 */
	public long getSize() {
		return size;
	}

	/**
	 * Method checks whether a file is sharded. An input file to HungryHippos
	 * will be always sharded on the basis of some key. That means there will be
	 * a replicated copy of that file.
	 * 
	 * @return boolean
	 */
	public boolean isSharded() {
		return sharded;
	}

	/**
	 * sets a file is sharded. if sharded is true => the file is an input file
	 * for HungryHippos else its an output file.
	 * 
	 * @param sharded
	 */
	public void setSharded(boolean sharded) {
		this.sharded = sharded;
	}

	/**
	 * Checks whether file contents are replicated in the system.
	 * 
	 * @return
	 */
	public boolean isReplicated() {
		return replicated;
	}

	/**
	 * Sets true if file contents are replicated in the system else false.
	 * 
	 * @param replicated
	 */
	public void setReplicated(boolean replicated) {
		this.replicated = replicated;
	}

	/**
	 * retrieves the replication number.
	 * 
	 * @return
	 */
	public int getReplicationNumber() {
		return replicationNumber;
	}

	/**
	 * Sets the replication Number.
	 * 
	 * @param replicationNumber
	 */
	public void setReplicationNumber(int replicationNumber) {
		this.replicationNumber = replicationNumber;
	}

	/**
	 * Retrieves the replicated file details.
	 * 
	 * @return
	 */
	public String[] getReplicatedFiles() {
		return replicatedFiles;
	}

	/**
	 * Sets the location of the files where replicatedfiles are sent.
	 * 
	 * @param replicatedFiles
	 */
	public void setReplicatedFiles(String[] replicatedFiles) {
		this.replicatedFiles = replicatedFiles;
	}

}
