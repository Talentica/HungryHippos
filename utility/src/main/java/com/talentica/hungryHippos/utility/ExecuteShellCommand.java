/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class ExecuteShellCommand {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ExecuteShellCommand.class);

	public static void main(String[] args) {
		try {
			if (args.length < 1) {
				LOGGER.info("Please provide argument for shell script");
				return;
			}
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec(new String[] { "/bin/bash -c", args[0] });

			BufferedReader input = new BufferedReader(new InputStreamReader(
					pr.getInputStream()));
			String line = "";
			while ((line = input.readLine()) != null) {
				LOGGER.info(line);
			}
		} catch (Exception e) {
			LOGGER.info("Execption {}",e);
		}
	}
}
