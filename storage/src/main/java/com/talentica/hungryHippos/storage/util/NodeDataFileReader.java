package com.talentica.hungryHippos.storage.util;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.xml.bind.JAXBException;

import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.FileDataStore;

/**
 * Utility class to read data files generated on nodes after sharding process is
 * completed for input.
 * 
 * @author nitink
 */
public class NodeDataFileReader {

	private static Logger LOGGER = LoggerFactory.getLogger(NodeDataFileReader.class);

	private static FieldTypeArrayDataDescription dataDescription;

	public static void main(String[] args) throws IOException, ClassNotFoundException, KeeperException, InterruptedException, JAXBException {
		if (args.length != 1) {
			System.out.println(
					"Usage pattern: java -jar <jar name> <path to parent folder of data folder> e.g. java -jar storage.jar ~/home/");
			System.exit(0);
		}
		int noOfKeys = ShardingApplicationContext.getShardingDimensions(args[0]).length;
		for (int i = 0; i < 1 << noOfKeys; i++) {
			String dataFileName = args[0] + FileSystemContext.getDataFilePrefix() + i;
			FileInputStream fileInputStream = new FileInputStream(new File(dataFileName));
			DataInputStream dataInputStream = new DataInputStream(fileInputStream);
			File readableDataFile = new File(dataFileName + "_read");
			FileWriter fileWriter = new FileWriter(readableDataFile);
			try {
				DynamicMarshal dynamicMarshal = getDynamicMarshal(args[0]);
				int noOfBytesInOneDataSet = dataDescription.getSize();
				while (dataInputStream.available() > 0) {
					byte[] bytes = new byte[noOfBytesInOneDataSet];
					dataInputStream.readFully(bytes);
					ByteBuffer buffer = ByteBuffer.wrap(bytes);
					for (int index = 0; index < dataDescription.getNumberOfDataFields(); index++) {
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

	private static DynamicMarshal getDynamicMarshal(String path) throws ClassNotFoundException, FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
		dataDescription = ShardingApplicationContext.getConfiguredDataDescription(path);
		dataDescription.setKeyOrder(ShardingApplicationContext.getShardingDimensions(path));
		DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
		return dynamicMarshal;
	}

}
