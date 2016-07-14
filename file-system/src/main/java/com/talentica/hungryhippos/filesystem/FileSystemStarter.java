package com.talentica.hungryhippos.filesystem;

import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.filesystem.FileSystemConfig;
import com.talentica.hungryhippos.filesystem.Exception.HungryHipposFileSystemException;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * This is a class for uploading the filesystem configuration xml in the
 * zookeeper Created by rajkishoreh on 8/7/16.
 */
public class FileSystemStarter {
	private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemStarter.class);

	/**
	 * This is the main method for uploading the file system configuration.
	 * Takes path to the filesystem configuration xml as argument
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			LOGGER.info("Uploading FileSystem configuration");
			validateArguments(args);
			String fileSystemConfigFilePath = args[0];
			FileSystemConfig configuration = JaxbUtil.unmarshalFromFile(fileSystemConfigFilePath,
					FileSystemConfig.class);
			if (configuration.getServerPort() == 0 || configuration.getFileStreamBufferSize() <= 0
					|| configuration.getQueryRetryInterval() <= 0 || configuration.getMaxQueryAttempts() <= 0
					|| configuration.getMaxClientRequests() <= 0 || configuration.getDataFilePrefix() == null
					|| configuration.getRootDirectory() == null || "".equals(configuration.getDataFilePrefix())
					|| "".equals(configuration.getRootDirectory())) {
				throw new RuntimeException(
						"Invalid configuration file or cluster configuration missing." + fileSystemConfigFilePath);
			}
			FileSystemContext
					.uploadFileSystemConfig(FileUtils.readFileToString(new File(fileSystemConfigFilePath), "UTF-8"));
			LOGGER.info("FileSystem configuration Upload compeleted");
		} catch (Exception exception) {
			LOGGER.error("Exception occured while Uploading FileSystem configuration", exception);
			throw new HungryHipposFileSystemException(exception.getMessage());
		}
	}

	/**
	 * Validates by checking the number of arguments
	 * 
	 * @param args
	 */
	private static void validateArguments(String[] args) {
		if (args == null || args.length < 1) {
			LOGGER.error("Missing filesystem configuration file arguments.");
			System.exit(1);
		}
	}

}
