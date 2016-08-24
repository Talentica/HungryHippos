package com.talentica.hungryHippos.droplet.main;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.droplet.DigitalOceanServiceImpl;
import com.talentica.hungryHippos.droplet.entity.DigitalOceanEntity;
import com.talentica.hungryHippos.droplet.util.DigitalOceanServiceUtil;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanManager {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(DigitalOceanManager.class);
	private static DigitalOceanServiceImpl dropletService;
	private static ObjectMapper mapper = new ObjectMapper();
	private static String jobUUId;

	public static void main(String[] args) throws Exception {/*
		try {
			if (args.length == 2) {
				PropertyOld.overrideConfigurationProperties(args[1]);
			} else if (args.length == 3) {
				jobUUId = args[2];
				CommonUtil.loadDefaultPath(jobUUId);
				PropertyOld.overrideConfigurationProperties(args[1]);
			} else {
				LOGGER.info("Please provide the argument.First argument is json,second argument is config file and third argument is optional for jobUUId");
				return;
			}
			validateProgramArguments(args);
			CoordinationApplicationContext.getProperty().getValueByKey("webserver.ip").toString();
			DigitalOceanEntity dropletEntity = getDropletEntity(args);
			dropletService = new DigitalOceanServiceImpl(
					dropletEntity.getAuthToken());
			DigitalOceanServiceUtil.performServices(dropletService,
					dropletEntity, jobUUId);
			if(dropletEntity.getRequest().getRequest().toUpperCase().equals("CREATE")){
				ScriptExecutionUtil.callCopySuccessShellScript(jobUUId);
			}
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException | RequestUnsuccessfulException
				| DigitalOceanException | IOException | InterruptedException e) {
			LOGGER.info("Unable to perform the operations {}", e);
			ScriptExecutionUtil.callCopyFailureShellScript(jobUUId);
		}
	*/}

	/**
	 * @param args
	 * @return
	 * @throws IOException
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 */
	private static DigitalOceanEntity getDropletEntity(String[] args)
			throws IOException, JsonParseException, JsonMappingException {
		String filePath = args[0];
		File file = new File(filePath);
		DigitalOceanEntity dropletEntity = mapper.readValue(file,
				DigitalOceanEntity.class);
		return dropletEntity;
	}

	private static void validateProgramArguments(String[] args)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		if (args.length < 1) {
			System.out.println("Please provide the json file as argument");
			System.exit(1);
		}
	}


}
