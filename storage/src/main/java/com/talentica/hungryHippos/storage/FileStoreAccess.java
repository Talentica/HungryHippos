package com.talentica.hungryHippos.storage;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * Created by debasishc on 31/8/15.
 */
public class FileStoreAccess implements StoreAccess {

	private static final Logger logger = LoggerFactory.getLogger("FileStoreAccess");
	private static String CANONICAL_PATH = null;
	private List<RowProcessor> rowProcessors = new ArrayList<>();
	private int keyId;
	private int numFiles;
	private String base;
	private ByteBuffer byteBuffer = null;
	private byte[] byteBufferBytes;

	static {
		try {
			CANONICAL_PATH = new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath();
		} catch (IOException ioe) {
			logger.error(ioe.getMessage());
		}
	}

	public FileStoreAccess(String base, int keyId, int numFiles, DataDescription dataDescription) {
		this.keyId = keyId;
		this.numFiles = numFiles;
		this.base = base;
		byteBufferBytes = new byte[dataDescription.getSize()];
		byteBuffer = ByteBuffer.wrap(byteBufferBytes);
	}

	@Override
	public void addRowProcessor(RowProcessor rowProcessor) {
		rowProcessors.add(rowProcessor);
	}

	@Override
	public void processRows() {
		try {
			int keyIdBit = 1 << keyId;
			for (int i = 0; i < numFiles; i++) {
				if ((keyIdBit & i) > 0) {
					processRows(i);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void processRows(int fileId) throws IOException {
		DataInputStream in = null;
		try {
			File dataFile = new File(CANONICAL_PATH + PathUtil.SEPARATOR_CHAR + base + fileId);
			in = new DataInputStream(new FileInputStream(dataFile));
			long dataFileSize = dataFile.length();
			while (dataFileSize > 0) {
				byteBuffer.clear();
				in.readFully(byteBufferBytes);
				dataFileSize = dataFileSize - byteBufferBytes.length;
				for (RowProcessor p : rowProcessors) {
					p.processRow(byteBuffer);
				}
				byteBuffer.flip();
			}
		} finally {
			closeDatsInputStream(in);
		}
	}

	private void closeDatsInputStream(DataInputStream in) throws IOException {
		if (in != null) {
			in.close();
		}
	}

	public void clear() {
		rowProcessors.clear();
	}

}
