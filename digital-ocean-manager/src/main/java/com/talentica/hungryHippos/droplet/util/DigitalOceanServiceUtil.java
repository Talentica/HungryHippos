package com.talentica.hungryHippos.droplet.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.httpclient.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.myjeeva.digitalocean.pojo.Account;
import com.myjeeva.digitalocean.pojo.Droplet;
import com.myjeeva.digitalocean.pojo.Droplets;
import com.myjeeva.digitalocean.pojo.Image;
import com.myjeeva.digitalocean.pojo.Images;
import com.myjeeva.digitalocean.pojo.Key;
import com.myjeeva.digitalocean.pojo.Keys;
import com.myjeeva.digitalocean.pojo.Network;
import com.myjeeva.digitalocean.pojo.Regions;
import com.myjeeva.digitalocean.pojo.Sizes;
import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.ServerHeartBeat;
import com.talentica.hungryHippos.coordination.domain.ZKNodeFile;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.droplet.DigitalOceanServiceImpl;
import com.talentica.hungryHippos.droplet.entity.DigitalOceanEntity;
import com.talentica.hungryHippos.droplet.query.JobRequest;
import com.talentica.hungryHippos.tester.api.job.Job;
import com.talentica.hungryHippos.tester.api.job.JobInput;
import com.talentica.hungryHippos.utility.PathUtil;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanServiceUtil {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DigitalOceanServiceUtil.class);
	private static NodesManager nodesManager = Property.getNodesManagerIntances();
	private static String ZK_IP;
	private static String OUTPUT_IP;
	private static int MAXIMUM_DROPLETS_IN_BATCH = 10;
	/** 
	 * @param dropletEntity
	 * @param dropletService
	 * @throws Exception
	 */
	public static void performServices(DigitalOceanServiceImpl dropletService,
			DigitalOceanEntity dropletEntity,String ...jobUUId) throws Exception {
		if(jobUUId != null && jobUUId.length == 1) CommonUtil.loadDefaultPath(jobUUId[0]);
		String[] dropletIds;
		int dropletId;
		Droplets droplets;
		Droplet droplet;
		Image image = null;
		Images images = null;
		//String dropletNamePattern = Property.getProperties().getProperty("common.droplet.name.pattern");
		String dropletNamePattern = jobUUId[0];
		switch (dropletEntity.getRequest()) {
		case CREATE:
			populatePresetValues(dropletService, dropletEntity, image);
			Droplets retDroplest = createDroplets(dropletService,
					dropletEntity, dropletNamePattern);
			if(jobUUId != null && jobUUId.length == 1)
				performConfigurationService(dropletService,retDroplest,jobUUId);
			else
				LOGGER.info("Please provide the jobUUId");
			break;

		case DELETE:
			for (String dpltId : getDropletIdsFile(jobUUId[0])) {
				if(dpltId == null) continue;
				try {
					dropletService.deleteDroplet(Integer.valueOf(dpltId));
					LOGGER.info("Destroy of droplet id {} is initiated.",
							dpltId);
				} catch (DigitalOceanException ex) {
					LOGGER.info(
							"Unable to delete the droplet id {} and exception {}",
							dpltId, ex);
				}
			}
			break;

		case RENAME:
			dropletIds = validateForSingalIdRequest(dropletEntity);
			if (dropletIds == null)
				return;
			dropletService.renameDroplet(Integer.valueOf(dropletIds[0]),
					dropletEntity.getDroplet().getName());
			LOGGER.info("Droplet id {} is renamed to {}", dropletEntity
					.getDroplet().getId(), dropletEntity.getDroplet().getName());
			break;

		case POWER_OFF_ON:
			dropletIds = validateForSingalIdRequest(dropletEntity);
			if (dropletIds == null)
				return;
			switch (dropletEntity.getPowerOffOnFlag()) {
			case 0:
				dropletService.powerOffDroplet(Integer.valueOf(dropletIds[0]));
				break;
			case 1:
				dropletService.powerOnDroplet(Integer.valueOf(dropletIds[0]));
				break;
			default:
				LOGGER.info("Please provide the off/on command in json file.");
			}
			break;

		case GET_ALL_DROPLET_INFO:
			droplets = dropletService.getAvailableDroplets(
					dropletEntity.getPageNo(), dropletEntity.getPerPage());
			List<Integer> dropletIdList = new ArrayList<>();
			for (Droplet dropletObj : droplets.getDroplets()) {
				if(!dropletObj.getName().contains(dropletNamePattern)) continue; 
				String[] nameArray = dropletObj.getName().split("-");
				if(nameArray.length < 2) continue;
				if(!dropletNamePattern.equalsIgnoreCase(nameArray[1])) continue;
				dropletIdList.add(dropletObj.getId());
				LOGGER.info("Droplet id {} , name {} , ip {}",
						dropletObj.getId(), dropletObj.getName(), dropletObj
								.getNetworks().getVersion4Networks().toString());
			}
			LOGGER.info("Droplet ids {}", dropletIdList);
			LOGGER.info("All droplets info are {}", droplets.toString());
			break;

		case GET_ALL_PROPERTIES_OF_DIGITAL_OCEAN:
			Account account = dropletService.getAccountInfo();
			LOGGER.info("Account info {}", account.toString());
			images = dropletService.getAvailableImages(
					dropletEntity.getPageNo(), dropletEntity.getPerPage());
			LOGGER.info("All images available {}", images.toString());
			Keys keys = dropletService.getAvailableKeys(dropletEntity
					.getPageNo());
			LOGGER.info("All Keys available {}", keys.toString());
			Regions regions = dropletService.getAvailableRegions(dropletEntity
					.getPageNo());
			LOGGER.info("All regions available {}", regions.toString());
			Sizes sizes = dropletService.getAvailableSizes(dropletEntity
					.getPageNo());
			LOGGER.info("All sizes available {}", sizes.toString());
			break;

		case SNAPSHOT:
			dropletIds = validateForSingalIdRequest(dropletEntity);
			if (dropletIds == null)
				return;
			dropletId = Integer.valueOf(dropletIds[0]);
			droplet = dropletService.getDropletInfo(dropletId);
			if (droplet.isActive()) {
				dropletService.powerOffDroplet(dropletId);
			}
			while (!droplet.isOff()) {
				Thread.sleep(5000);
				droplet = dropletService.getDropletInfo(dropletId);
				LOGGER.info("Wating for power off of droplet id {}", dropletId);
			}
			LOGGER.info("Droplet is shutdown now start takeing snapshot.");
			if (dropletEntity.getSnapshotName() == null
					|| "".equalsIgnoreCase(dropletEntity.getSnapshotName())) {
				dropletService.takeDropletSnapshot(dropletId);
			} else {
				dropletService.takeDropletSnapshot(dropletId,
						dropletEntity.getSnapshotName());
			}
			LOGGER.info("Snapshot is initiated successfully.");
			break;

		case SHUTDOWN:
			dropletIds = validateForSingalIdRequest(dropletEntity);
			if (dropletIds == null)
				return;
			dropletId = Integer.valueOf(dropletIds[0]);
			droplet = dropletService.getDropletInfo(dropletId);
			if (droplet.isActive()) {
				dropletService.shutdownDroplet(dropletId);
			}
			LOGGER.info("Droplets are shutdown");
			break;

		case CREATE_KEY:
			for (Key key : dropletEntity.getDroplet().getKeys()) {
				dropletService.createKey(key);
			}
			LOGGER.info("Key is added");
			break;

		default:
			LOGGER.info("No services is performed");
			break;

		}
	}

	/**
	 * @param dropletService
	 * @param dropletEntity
	 * @param image
	 * @throws DigitalOceanException
	 * @throws RequestUnsuccessfulException
	 */
	private static void populatePresetValues(
			DigitalOceanServiceImpl dropletService,
			DigitalOceanEntity dropletEntity, Image image)
			throws DigitalOceanException, RequestUnsuccessfulException {
		Images images;
		if (dropletEntity.getDroplet().getImage().getId() != null) {
			image = dropletService.getImageInfo(dropletEntity.getDroplet()
					.getImage().getId());
		} else if (dropletEntity.getDroplet().getImage().getName() != null
				|| "".equals(dropletEntity.getDroplet().getImage().getId())) {
			boolean isImageFound = false;
			for (int page = 1;; page++) {
				images = dropletService.getAvailableImages(page, 100);
				if (images.getImages().isEmpty())
					break;
				for (Image img : images.getImages()) {
					if (dropletEntity.getDroplet().getImage().getName()
							.equalsIgnoreCase(img.getName())) {
						LOGGER.info("Image id {} and name {}", img.getId(),
								img.getName());
						image = img;
						isImageFound = true;
						break;
					}
				}
				if (isImageFound)
					break;
			}
		}
		if (image != null) {
			dropletEntity.getDroplet().setImage(image);
		}
		List<Key> newKeys = new ArrayList<Key>();
		Keys fetchKeys = dropletService.getAvailableKeys(null);
		if (fetchKeys != null && fetchKeys.getKeys() != null
				&& !fetchKeys.getKeys().isEmpty()) {
			newKeys.addAll(fetchKeys.getKeys());
		} else {
			LOGGER.info("No keys are available");
		}
		dropletEntity.getDroplet().setKeys(newKeys);
	}

	/**
	 * @param dropletService
	 * @param dropletEntity
	 * @param dropletNamePattern
	 * @return
	 * @throws DigitalOceanException
	 * @throws RequestUnsuccessfulException
	 */
	private static Droplets createDroplets(
			DigitalOceanServiceImpl dropletService,
			DigitalOceanEntity dropletEntity, String dropletNamePattern)
			throws DigitalOceanException, RequestUnsuccessfulException {
		Droplets droplets;
		List<String> newNames = new ArrayList<>();
		int onOfDroplets = Integer.valueOf(Property.getProperties().getProperty("common.no.of.droplets"));
		String PRIFIX = "hh";
		String HYPHEN = "-";
		List<String> names = dropletEntity.getDroplet().getNames();
		// for master and output droplets default
		for(String name : names){
			newNames.add(name.replaceAll("hh", "hh"+"-"+dropletNamePattern));
		}
		dropletEntity.getDroplet().getNames().clear();
		dropletEntity.getDroplet().getNames().addAll(newNames);
		Droplets retDroplest = new Droplets();
		List<Droplet> dropletList = new ArrayList<>();
		for(int index = 0 ; index < onOfDroplets ; index ++ ){
			dropletEntity.getDroplet().getNames().add(PRIFIX + HYPHEN + dropletNamePattern + HYPHEN + index);
			if(dropletEntity.getDroplet().getNames().size() == MAXIMUM_DROPLETS_IN_BATCH){
				droplets = dropletService
						.createDroplets(dropletEntity.getDroplet());
				dropletList.addAll(droplets.getDroplets());
				dropletEntity.getDroplet().getNames().clear();
			}
		}
		
		if(dropletEntity.getDroplet().getNames().size() != 0 && dropletEntity.getDroplet().getNames().size() < 10){
			droplets = dropletService
					.createDroplets(dropletEntity.getDroplet());
			dropletList.addAll(droplets.getDroplets());
			dropletEntity.getDroplet().getNames().clear();
		}
		retDroplest.setDroplets(dropletList);
		LOGGER.info("Droplet/Droplets is/are of id/ids {} is initiated",
				retDroplest.toString());
		return retDroplest;
	}
	
	public static List<String> getDropletIdsFile(String jobUUId) throws IOException {
		CommonUtil.loadDefaultPath(jobUUId);
		return CommonUtil.readFile(new File(CommonUtil.DROPLET_IDS_FILE_PATH));
	}

	/**
	 * @param dropletService
	 * @throws DigitalOceanException
	 * @throws RequestUnsuccessfulException
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws Exception
	 */
	private static void performConfigurationService(DigitalOceanServiceImpl dropletService,Droplets droplets,String ...jobUUId)
			throws DigitalOceanException, RequestUnsuccessfulException,
			InterruptedException, IOException, Exception {
		
		String formatFlag = Property.getZkPropertyValue(
				"zk.cleanup.zookeeper.nodes").toString();
		/*List<String> jobuuid = new ArrayList<String>();
		jobuuid.add(jobUUId[0]);
		writeLineInFile(CommonUtil.JOB_UUID_FILE_PATH,jobuuid);
		LOGGER.info("job uuid file is cleated in path {}",CommonUtil.JOB_UUID_FILE_PATH);*/
		if (Property.getNamespace().name().equalsIgnoreCase("zk")
				&& formatFlag.equals("Y")) {
			List<Droplet> dropletFill = getActiveDroplets(dropletService,
					droplets);
			LOGGER.info("Active droplets are {}", dropletFill.toString());
			LOGGER.info("Generating server conf file");
			List<String> ipv4AddrsList = generateServerConfigFile(dropletFill,jobUUId[0]);
			LOGGER.info("IP Address {}",ipv4AddrsList);
			LOGGER.info("Generating server config file");
			writeLineInFile(CommonUtil.TEMP_JOBUUID_FOLDER_PATH+Property.SERVER_CONF_FILE, ipv4AddrsList);
			LOGGER.info("Server config file is created...");
			LOGGER.info("Start zookeeper server");
			startZookeeperServer(jobUUId[0]);
			LOGGER.info("Zookeeper server started...");
			LOGGER.info("Creating default nodes");
			if (nodesManager == null) {
				CommonUtil.loadDefaultPath(jobUUId[0]);
				String zkIp = CommonUtil.getZKIp();
				LOGGER.info("zk ip is {}",zkIp);
				nodesManager= ServerHeartBeat.init().connectZookeeper(zkIp);
			} 
			nodesManager.startup();
			LOGGER.info("Default nodes are created...");
			LOGGER.info("Uploading server conf file to zk node");
			uploadServerConfigFileToZK();
			LOGGER.info("Server conf file is uploaded");
			LOGGER.info("Uploading dynamic conf file to zk node");
			uploadDynamicConfigFileToZk(getPropertyKeyValueFromJobByHHTPRequest(jobUUId[0]));
			// uploadDynamicConfigFileToZk(getHardCodePropertyKeyValueFromJobByHHTPRequest(jobUUId[0]));
			LOGGER.info("Conf file is uploaded...");
			List<String> webServerIp = new ArrayList<String>();
			webServerIp.add(Property.getProperties().get("common.webserver.ip").toString());
			writeLineInFile(CommonUtil.WEBSERVER_IP_FILE_PATH, webServerIp);
		}
	}
	
	private static Map<String, String> getPropertyKeyValueFromJobByHHTPRequest(String jobUUId)
			throws HttpException, IOException {
		Map<String,String> keyValue = new HashMap<String, String>();
		
		JobRequest jobRequest = new JobRequest();
		Job job = jobRequest.getJobDetails(jobUUId);
		JobInput jobInput = job.getJobInput();
		
		keyValue.put("input.file.url.link",jobInput.getDataLocation());
		keyValue.put("common.sharding_dimensions",jobInput.getShardingDimensions());
		keyValue.put("column.datatype-size",jobInput.getDataTypeConfiguration());
		keyValue.put("column.file.size",jobInput.getDataSize().toString());
		keyValue.put("job.matrix.class",jobInput.getJobMatrixClass());
		keyValue.put("job.uuid",jobUUId);
		return keyValue;
	}
	
	private static Map<String, String> getHardCodePropertyKeyValueFromJobByHHTPRequest(String jobUUId)
			throws HttpException, IOException {
		Map<String,String> keyValue = new HashMap<String, String>();
		keyValue.put("input.file.url.link","http://192.241.248.197/input/sampledata.txt");
		keyValue.put("common.sharding_dimensions","key1,key2,key3");
		keyValue.put("column.datatype-size","STRING-1,STRING-1,STRING-1,STRING-3,STRING-3,STRING-3,DOUBLE-0,DOUBLE-0,STRING-5");
		keyValue.put("input.file.size","55537404");
		keyValue.put("job.matrix.class","com.talentica.hungryHippos.test.sum.SumJobMatrixImpl");
		keyValue.put("job.uuid","NzFiNzdlM2MtMDgwMC00N2M3LTkzOTgtN2Y1YWU4ZmQ5T");
		return keyValue;
	}

	/**
	 * @param digitalOceanManager
	 * @throws IOException
	 */
	private static void uploadDynamicConfigFileToZk(Map<String, String> keyValue)
			throws IOException {
		Properties properties = Property.getProperties();
		properties.setProperty("zookeeper.server.ips", ZK_IP + ":2181");
		for (Entry<String, String> entry : keyValue.entrySet()) {
			properties.setProperty(entry.getKey(), entry.getValue());
		}
		ZKNodeFile mergedConfigNodeFile = new ZKNodeFile(Property.MERGED_CONFIG_PROP_FILE, properties);
		nodesManager.saveConfigFileToZNode(mergedConfigNodeFile, null);
		
	}

	/**
	 * @param digitalOceanManager
	 * @throws IOException
	 */
	private static void uploadServerConfigFileToZK() throws IOException {
		LOGGER.info("PUT THE CONFIG FILE TO ZK NODE");
		ZKNodeFile serverConfigFile = new ZKNodeFile(Property.SERVER_CONF_FILE,
				Property.loadServerProperties());
		nodesManager.saveConfigFileToZNode(serverConfigFile, null);
		LOGGER.info("serverConfigFile file successfully put on zk node.");
	}

	/**
	 * @throws IOException
	 */
	private static void startZookeeperServer(String jobuuid) throws IOException {
		LOGGER.info("Executing shell command to start the zookeeper");
		String zkScriptPath = Paths.get("../bin").toAbsolutePath().toString()+PathUtil.FORWARD_SLASH;
		String[] strArr = new String[] {"/bin/sh",zkScriptPath+"start-zk-server.sh",jobuuid};
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("Shell command is executed");
	}
	
	/*private static void startKazooServer(){
		String uuidForTesting = "NzFiNzdlM2MtMDgwMC00N2M3LTkzOTgtN2Y1YWU4ZmQ5A"; //need to remove
		LOGGER.info("START THE KAZOO TO MONITOR THE NODES FOR FINISH");
		String zkScriptPath = Paths.get("../bin").toAbsolutePath().toString()+PathUtil.FORWARD_SLASH;
		String[] strArr = new String[] {"/bin/sh",zkScriptPath+"start-kazoo-server.sh",uuidForTesting};
		CommonUtil.executeScriptCommand("/usr/bin/python","/root/hungryhippos/scripts/python_scripts/"+"start-kazoo-server.py"+" "+CommonUtil.getJobUUIdInBase64());
		CommonUtil.executeScriptCommand(strArr);
		LOGGER.info("KAZOO SERVER IS STARTED");
	}*/
	

	/**
	 * @param dropletService
	 * @param droplets
	 * @return
	 * @throws InterruptedException
	 * @throws DigitalOceanException
	 * @throws RequestUnsuccessfulException
	 */
	private static List<Droplet> getActiveDroplets(
			DigitalOceanServiceImpl dropletService, Droplets droplets)
			throws InterruptedException, DigitalOceanException,
			RequestUnsuccessfulException {
		LOGGER.info("Start getting active droplets...");
		String dropletNamePattern = Property.getProperties().getProperty("common.droplet.name.pattern");
		LOGGER.info("Droplet name pattern {}",dropletNamePattern);
		List<Droplet> dropletFill = new ArrayList<>();
		for (Droplet retDroplet : droplets.getDroplets()) {
			if (!retDroplet.getName().contains(dropletNamePattern))
				continue;
			retDroplet = dropletService.getDropletInfo(retDroplet
					.getId());
			if (!retDroplet.isActive()) {
				while (!retDroplet.isActive()) {
					LOGGER.info("Waiting for droplets to get active...");
					Thread.sleep(60000);
					retDroplet = dropletService.getDropletInfo(retDroplet
							.getId());
				}
			}
			dropletFill.add(retDroplet);
		}
		return dropletFill;
	}

	/**
	 * @param droplets
	 * @throws IOException
	 */
	private static List<String> generateServerConfigFile(
			List<Droplet> droplets,String jobUUId) throws IOException {
		int index = 0;
		String PRIFIX = "server.";
		String SUFFIX = ":";
		String PORT = "2324";
		Integer masterDropletId = null;
		List<String> ipAndPort = new ArrayList<>();
		List<String> serverIps = new ArrayList<>();
		List<String> dropletIdToBeDeleted = new ArrayList<>();
		for (Droplet retDroplet : droplets) {
			List<Network> networks = retDroplet.getNetworks()
					.getVersion4Networks();
			for (Network network : networks) {
				if (network.getType().equalsIgnoreCase("public")) {
					if (retDroplet.getName().contains("master")) {
						masterDropletId = retDroplet.getId();
						ZK_IP = network.getIpAddress();
						ipAndPort.clear();
						ipAndPort.add(ZK_IP);
						writeLineInFile(CommonUtil.MASTER_IP_FILE_NAME_ABSOLUTE_PATH,
								ipAndPort);
						break;
					}else if(retDroplet.getName().contains("output")){
						OUTPUT_IP = network.getIpAddress();
						ipAndPort.clear();
						ipAndPort.add(OUTPUT_IP);
						writeLineInFile(CommonUtil.OUTPUT_IP_FILE_NAME_ABSOLUTE_PATH, ipAndPort);
						break;
					}
					serverIps.add(PRIFIX + (index++)+":"+ network.getIpAddress()+ SUFFIX + PORT);
					dropletIdToBeDeleted.add(String.valueOf(retDroplet.getId()));
					break;
				}
			}
		}
		dropletIdToBeDeleted.add(String.valueOf(masterDropletId));
		writeLineInFile(CommonUtil.DROPLET_IDS_FILE_PATH,dropletIdToBeDeleted);
		return serverIps;
	}

	/**
	 * @param fileName
	 * @param lines
	 * @throws IOException
	 */
	public static void writeLineInFile(String fileName, List<String> lines)
			throws IOException {
		File fout = new File(fileName);
		LOGGER.info("Path {}", fout.getAbsolutePath());
		fout.createNewFile();
		FileOutputStream fos = new FileOutputStream(fout, false);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		int totalLine = lines.size();
		for (String line : lines) {
			totalLine--;
			bw.write(line);
			if (totalLine != 0)
				bw.newLine();
		}
		bw.close();
	}

	/**
	 * @param dropletEntity
	 * @return
	 */
	private static String[] validateForSingalIdRequest(
			DigitalOceanEntity dropletEntity) {
		String[] dropletIds = dropletEntity.getIds().split(",");
		if (dropletIds.length > 1) {
			LOGGER.info("More than one id is provided.");
			return null;
		}
		return dropletIds;
	}

}
