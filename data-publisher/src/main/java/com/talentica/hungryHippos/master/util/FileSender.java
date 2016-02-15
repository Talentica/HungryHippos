/**
 * 
 */
package com.talentica.hungryHippos.master.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class FileSender {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileSender.class);
	
	public static String execScript(String command) throws IOException, InterruptedException{
		LOGGER.info("Executing script: {}", command);
		StringBuffer output = new StringBuffer();
		Process p = Runtime.getRuntime().exec(new String[] { command });
		p.waitFor();
		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = "";
		while ((line = reader.readLine()) != null) {
			output.append(line + "\n");
		}
		LOGGER.info("Script executed");
		return output.toString();
	}
}
