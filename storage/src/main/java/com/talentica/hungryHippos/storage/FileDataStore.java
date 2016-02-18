package com.talentica.hungryHippos.storage;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * Created by debasishc on 31/8/15.
 */
public class FileDataStore implements DataStore, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7726551156576482829L;
	private static final Logger LOGGER = LoggerFactory.getLogger(FileDataStore.class);
	private final int numFiles;
	private NodeDataStoreIdCalculator nodeDataStoreIdCalculator;
	private OutputStream[] os;
	private DataDescription dataDescription;

	public static String DATA_FILE_BASE_NAME = "data" + File.separator + "data_";

	public FileDataStore(int numDimensions, NodeDataStoreIdCalculator nodeDataStoreIdCalculator,
			DataDescription dataDescription) throws IOException {
		this(numDimensions, nodeDataStoreIdCalculator, dataDescription, false);
	}

	public FileDataStore(int numDimensions, NodeDataStoreIdCalculator nodeDataStoreIdCalculator,
			DataDescription dataDescription, boolean readOnly) throws IOException {
		this.numFiles = 1 << numDimensions;
		this.nodeDataStoreIdCalculator = nodeDataStoreIdCalculator;
		this.dataDescription = dataDescription;
		os = new OutputStream[numFiles];
		if (!readOnly) {
			for (int i = 0; i < numFiles; i++) {
				os[i] = new BufferedOutputStream(
						new FileOutputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + File.separator
								+ DATA_FILE_BASE_NAME + i));
			}
		}
	}

	@Override
	public void storeRow(ByteBuffer row, byte[] raw) {
		int storeId = nodeDataStoreIdCalculator.storeId(row);
		try {
			os[storeId].write(raw);
		} catch (IOException e) {
			LOGGER.error("Error occurred while writing data received to datastore.", e);
		}
	}

	@Override
	public StoreAccess getStoreAccess(int keyId) {
		return new FileStoreAccess(DATA_FILE_BASE_NAME, keyId, numFiles, dataDescription);
	}

	@Override
	public void sync() {
		for (int i = 0; i < numFiles; i++) {
			try {
				os[i].flush();
			} catch (IOException e) {
				LOGGER.error("Error occurred while flushing " + i + "th outputstream.", e);
			} finally {
				try {
					if (os[i] != null)
						os[i].close();
				} catch (IOException e) {
					LOGGER.warn("\n\tUnable to close the connection; exception :: " + e.getMessage());
				}
			}
		}
	}

}
