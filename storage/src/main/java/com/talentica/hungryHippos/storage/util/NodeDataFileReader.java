package com.talentica.hungryHippos.storage.util;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

/**
 * Utility class to read data files generated on nodes after sharding process is
 * completed for input.
 * 
 * @author nitink
 */
public class NodeDataFileReader {

	private static Logger LOGGER = LoggerFactory.getLogger(NodeDataFileReader.class);

	private static FieldTypeArrayDataDescription dataDescription;

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println(
					"Usage pattern: java -jar <jar name> <input file path> <readable output file path> e.g. java -jar storage.jar ./data_0 ./data_0_readable");
			System.exit(0);
		}
		File dataFile = new File(args[0]);
		FileInputStream fileInputStream = new FileInputStream(dataFile);
		DataInputStream dataInputStream = new DataInputStream(fileInputStream);
		File readableDataFile = new File(args[1]);
		FileWriter fileWriter = new FileWriter(readableDataFile);
		try {
			DynamicMarshal dynamicMarshal = getDynamicMarshal();
			int noOfBytesInOneDataSet = dataDescription.getSize();
			long fileLength = dataFile.length();
			long bytesRead = 0;
			while (true) {
				if (bytesRead >= fileLength) {
					break;
				}
				byte[] bytes = new byte[noOfBytesInOneDataSet];
				bytesRead = bytesRead + noOfBytesInOneDataSet;
				dataInputStream.readFully(bytes);
				ByteBuffer buffer = ByteBuffer.wrap(bytes);
				for (int index = 0; index < 9; index++) {
					Object readableData = dynamicMarshal.readValue(index, buffer);
					if (index != 0) {
						fileWriter.write(",");
					}
					fileWriter.write(readableData.toString());
				}
				fileWriter.write("\n");
			}
		} finally {
			fileWriter.flush();
			fileWriter.close();
			fileInputStream.close();
		}
		LOGGER.info("Output readable data file is written to: " + readableDataFile.getAbsolutePath());
	}

	private static DynamicMarshal getDynamicMarshal() {
		dataDescription = new FieldTypeArrayDataDescription();
		dataDescription.addFieldType(DataLocator.DataType.STRING, 2);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 2);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 2);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
		dataDescription.addFieldType(DataLocator.DataType.DOUBLE, 0);
		dataDescription.addFieldType(DataLocator.DataType.DOUBLE, 0);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
		dataDescription.setKeyOrder(Property.getKeyOrder());
		DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
		return dynamicMarshal;
	}

}
