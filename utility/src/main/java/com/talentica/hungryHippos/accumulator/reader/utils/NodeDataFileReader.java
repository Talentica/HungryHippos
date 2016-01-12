package com.talentica.hungryHippos.accumulator.reader.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.talentica.hungryHippos.utility.marshaling.DataLocator;
import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

public class NodeDataFileReader {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println(
					"Usage pattern: java -jar <jar name> <input file path> <readable output file path> e.g. java -jar Utility.jar ./data_0 ./data_0_readable");
		}
		File dataFile = new File(args[0]);
		FileInputStream fileInputStream = new FileInputStream(dataFile);
		byte[] bytes = new byte[(int) dataFile.length()];
		fileInputStream.read(bytes);
		fileInputStream.close();
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		File readableDataFile = new File(args[1]);
		FileWriter fileWriter = new FileWriter(readableDataFile);
		DynamicMarshal dynamicMarshal = getDynamicMarshal();
		for (int index = 0; index < 9; index++) {
			Object readableData = dynamicMarshal.readValue(index, buffer);
			if(index!=0){
				fileWriter.write(",");	
			}
			fileWriter.write(readableData.toString());
		}
		fileWriter.flush();
		fileWriter.close();
	}

	private static DynamicMarshal getDynamicMarshal() {
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
		dataDescription.addFieldType(DataLocator.DataType.STRING, 2);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 2);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 2);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
		dataDescription.addFieldType(DataLocator.DataType.DOUBLE, 0);
		dataDescription.addFieldType(DataLocator.DataType.DOUBLE, 0);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
		dataDescription.setKeyOrder(new String[] { "key1", "key2", "key3" });
		DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
		return dynamicMarshal;
	}

}
