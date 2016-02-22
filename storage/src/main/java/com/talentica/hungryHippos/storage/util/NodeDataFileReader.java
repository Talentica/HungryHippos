package com.talentica.hungryHippos.storage.util;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.utility.CommonUtil;
import com.talentica.hungryHippos.utility.Property;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

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
		if (args.length != 1) {
			System.out.println(
					"Usage pattern: java -jar <jar name> <path to parent folder of data folder> e.g. java -jar storage.jar ~/home/");
			System.exit(0);
		}
		int noOfKeys = Property.getKeyOrder().length;
		for (int i = 0; i < 1 << noOfKeys; i++) {
			String dataFileName = args[0] + FileDataStore.DATA_FILE_BASE_NAME + i;
			FileInputStream fileInputStream = new FileInputStream(new File(dataFileName));
			DataInputStream dataInputStream = new DataInputStream(fileInputStream);
			File readableDataFile = new File(dataFileName + "_read");
			FileWriter fileWriter = new FileWriter(readableDataFile);
			try {
				DynamicMarshal dynamicMarshal = getDynamicMarshal();
				int noOfBytesInOneDataSet = dataDescription.getSize();
				while (dataInputStream.available() > 0) {
					byte[] bytes = new byte[noOfBytesInOneDataSet];
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
	}

	private static DynamicMarshal getDynamicMarshal() {
		FieldTypeArrayDataDescription dataDescription = CommonUtil.getConfiguredDataDescription();
		dataDescription.setKeyOrder(Property.getKeyOrder());
		DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
		return dynamicMarshal;
	}

}
