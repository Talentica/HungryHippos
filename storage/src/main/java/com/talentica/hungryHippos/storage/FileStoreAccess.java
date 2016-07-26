package com.talentica.hungryHippos.storage;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.sun.org.apache.xalan.internal.lib.NodeInfo;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.utility.PathUtil;

import javax.xml.bind.JAXBException;

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
	private String hungryHippoFilePath;
	private String nodeId;

	static {
		try {
			CANONICAL_PATH = new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath();
		} catch (IOException ioe) {
			logger.error(ioe.getMessage());
		}
	}

	public FileStoreAccess(String hungryHippoFilePath, String nodeId, String base, int keyId, int numFiles, DataDescription dataDescription) {
		this.hungryHippoFilePath = hungryHippoFilePath;
		this.nodeId = nodeId;
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
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void processRows(int fileId) throws Exception {
		DataInputStream in = null;
		File dataFile = null;
		try {
			dataFile = new File(FileSystemContext.getRootDirectory()+hungryHippoFilePath+File.separator + base + fileId);
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
			HungryHipposFileSystem.getInstance().updateFSBlockMetaData(hungryHippoFilePath, nodeId, dataFile.length());
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
