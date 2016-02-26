package com.talentica.hungryHippos.storage;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * Created by debasishc on 31/8/15.
 */
public class FileStoreAccess implements StoreAccess {

	private List<RowProcessor> rowProcessors = new ArrayList<>();
	private int keyId;
	private int numFiles;
	private String base;
	private ByteBuffer byteBuffer = null;
	private byte[] byteBufferBytes;

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
			in = new DataInputStream(new FileInputStream(
					new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH + base + fileId));
			while (true) {
				if (in.available() <= 0) {
					break;
				}
				byteBuffer.clear();
				in.readFully(byteBufferBytes);
				for (RowProcessor p : rowProcessors) {
					p.processRow(byteBuffer);
				}
				byteBuffer.flip();
			}
		} finally {
			closeDatsInputStream(in);
		}
	}

	@Override
	public void processRowCount() {
		try {
			int keyIdBit = 1 << keyId;
			for (int i = 0; i < numFiles; i++) {
				if ((keyIdBit & i) > 0) {
					processRowCount(i);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void processRowCount(int fileId) throws FileNotFoundException, IOException {
		DataInputStream in = null;
		try {
			in = new DataInputStream(new FileInputStream(
					new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath() + PathUtil.FORWARD_SLASH + base + fileId));
			while (true) {
				byteBuffer.clear();
				if (in.available() <= 0) {
					break;
				}
				in.readFully(byteBufferBytes);
				for (RowProcessor p : rowProcessors) {
					p.processRowCount(byteBuffer);
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
