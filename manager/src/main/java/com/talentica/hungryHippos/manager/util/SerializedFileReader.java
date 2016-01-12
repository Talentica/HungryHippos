package com.talentica.hungryHippos.manager.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang.SerializationUtils;

public class SerializedFileReader {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println(
					"Usage pattern: java -jar <jar name> <input file path> <readable output file path> e.g. java -jar Utility.jar ./serialized_data_0 ./readable_data_0");
			System.exit(0);
		}
		File dataFile = new File(args[0]);
		FileInputStream fileInputStream = new FileInputStream(dataFile);
		Object deserializedObject = SerializationUtils.deserialize(fileInputStream);
		fileInputStream.close();

		File readableDataFile = new File(args[1]);
		FileWriter fileWriter = new FileWriter(readableDataFile);
		if (deserializedObject != null) {
			fileWriter.write(deserializedObject.toString().toString());
		}
		fileWriter.flush();
		fileWriter.close();
	}

}
