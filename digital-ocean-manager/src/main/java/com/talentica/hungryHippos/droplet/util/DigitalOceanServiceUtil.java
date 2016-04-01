package com.talentica.hungryHippos.droplet.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.myjeeva.digitalocean.pojo.Account;
import com.myjeeva.digitalocean.pojo.Delete;
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
import com.talentica.hungryHippos.droplet.DigitalOceanServiceImpl;
import com.talentica.hungryHippos.droplet.entity.DigitalOceanEntity;
import com.talentica.hungryHippos.utility.PathUtil;
import com.talentica.hungryHippos.utility.Property;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanServiceUtil {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DigitalOceanServiceUtil.class);
	private static String configPath = new File(System.getProperty("user.dir")).getParent()+File.separator+"src/main/resources";
	private static NodesManager nodesManager;

	/**
	 * @param dropletEntity
	 * @param dropletService
	 * @throws Exception
	 */
	public static void performServices(DigitalOceanServiceImpl dropletService,
			DigitalOceanEntity dropletEntity) throws Exception {
		String[] dropletIds;
		int dropletId;
		Droplets droplets;
		Droplet droplet;
		Image image = null;
		Images images = null;
		switch (dropletEntity.getRequest()) {
		case CREATE:
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
			droplets = dropletService
					.createDroplets(dropletEntity.getDroplet());
			LOGGER.info("Droplet/Droplets is/are of id/ids {} is initiated",
					droplets.toString());
			String formatFlag = Property.getPropertyValue(
					"cleanup.zookeeper.nodes").toString();
			if (Property.getNamespace().name().equalsIgnoreCase("zk")
					&& formatFlag.equals("Y")) {
				droplets = dropletService.getAvailableDroplets(1, 20);
				List<Droplet> dropletFill = getActiveDroplets(dropletService,
						droplets);
				LOGGER.info("Active droplets are {}", dropletFill.toString());
				generateServerConfigFile(dropletFill);
				startZookeeper();
				uploadServerConfigFileToZK();
				uploadConfigFileToZk();
			}
			break;

		case DELETE:
			List<Delete> deleteList = dropletService
					.deleteDroplets(dropletEntity.getIdsAsList());
			LOGGER.info("The list are deleted {}", deleteList.toString());
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
	 * @param digitalOceanManager
	 * @throws IOException
	 */
	private static void uploadConfigFileToZk() throws IOException {
		ZKNodeFile configNodeFile = new ZKNodeFile(Property.CONF_PROP_FILE
				+ "_FILE", Property.getProperties());
		nodesManager.saveConfigFileToZNode(configNodeFile, null);
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
	 * @param digitalOceanManager
	 * @throws Exception
	 */
	private static void startZookeeper() throws Exception {
		(nodesManager = ServerHeartBeat.init()).startup();
	}

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
		List<Droplet> dropletFill = new ArrayList<>();
		for (Droplet retDroplet : droplets.getDroplets()) {
			if (!retDroplet.getName().contains("node"))
				continue;
			while (!retDroplet.isActive()) {
				LOGGER.info("Waiting for droplets to get active...");
				Thread.sleep(10000);
				retDroplet = dropletService.getDropletInfo(retDroplet.getId());
			}
			dropletFill.add(retDroplet);
		}
		return dropletFill;
	}

	/**
	 * @param droplets
	 */
	private static void generateServerConfigFile(List<Droplet> droplets) {
		List<String> ipv4Addrs;
		ipv4Addrs = new ArrayList<String>();
		int index = 0;
		String PRIFIX = "server.";
		String SUFFIX = ":";
		String PORT = "2324";
		for (Droplet retDroplet : droplets) {
			List<Network> networks = retDroplet.getNetworks()
					.getVersion4Networks();
			for (Network network : networks) {
				if (network.getType().equalsIgnoreCase("public")) {
					ipv4Addrs.add(PRIFIX + (index++) + SUFFIX
							+ network.getIpAddress() + SUFFIX + PORT);
					break;
				}
			}
		}
		try {
			LOGGER.info("Resource path {}", configPath);
			configPath = (configPath.endsWith(PathUtil.FORWARD_SLASH) ? (configPath + Property.SERVER_CONF_FILE)
					: (configPath + PathUtil.FORWARD_SLASH + Property.SERVER_CONF_FILE));
			LOGGER.info("serverConfigFile.properties path will be saved in path {}",configPath);
			writeLineInFile(configPath, ipv4Addrs);
			LOGGER.info("serverConfigFile.properties file is create successfully");
		} catch (IOException e) {
			LOGGER.info(
					"Unable to write the servers ips in serverConfigFile.properties file ,exception {}",
					e);
		}
	}

	public static void writeLineInFile(String fileName, List<String> lines)
			throws IOException {
		File fout = new File(fileName);
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
