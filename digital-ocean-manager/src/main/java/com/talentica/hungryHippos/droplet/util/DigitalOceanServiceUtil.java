/**
 * 
 */
package com.talentica.hungryHippos.droplet.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myjeeva.digitalocean.common.DropletStatus;
import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.myjeeva.digitalocean.pojo.Account;
import com.myjeeva.digitalocean.pojo.Delete;
import com.myjeeva.digitalocean.pojo.Droplet;
import com.myjeeva.digitalocean.pojo.Droplets;
import com.myjeeva.digitalocean.pojo.Image;
import com.myjeeva.digitalocean.pojo.Images;
import com.myjeeva.digitalocean.pojo.Keys;
import com.myjeeva.digitalocean.pojo.Regions;
import com.myjeeva.digitalocean.pojo.Sizes;
import com.talentica.hungryHippos.droplet.DigitalOceanServiceImpl;
import com.talentica.hungryHippos.droplet.entity.DigitalOceanEntity;
import com.talentica.hungryHippos.utility.CommonUtil;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanServiceUtil {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DigitalOceanServiceUtil.class);

	/**
	 * @param dropletEntity
	 * @param dropletService
	 * @throws DigitalOceanException
	 * @throws RequestUnsuccessfulException
	 * @throws InterruptedException 
	 */
	public static void performServices(DigitalOceanServiceImpl dropletService,
			DigitalOceanEntity dropletEntity) throws DigitalOceanException,
			RequestUnsuccessfulException, InterruptedException {
		String[] dropletIds;
		int dropletId;
		Droplets droplets;
		Droplet droplet;
		Image image = null;
		switch (dropletEntity.getRequest()) {
		case CREATE:
			if (dropletEntity.getDroplet() != null
					&& dropletEntity.getDroplet().getImage() != null) {
				image = dropletService.getImageInfo(dropletEntity.getDroplet()
						.getImage().getSlug());
			}
			if (image != null) {
				dropletEntity.getDroplet().setImage(image);
			}
			droplets = dropletService
					.createDroplets(dropletEntity.getDroplet());
			LOGGER.info("Droplet/Droplets is/are of id/ids {} is created",
					droplets.toString());
			droplets = dropletService.getAvailableDroplets(1, null);
			List<Droplet> dropletFill  = new ArrayList<>();
			for(Droplet retDroplet : droplets.getDroplets()){
				while(!DropletStatus.ACTIVE.toString().equalsIgnoreCase(retDroplet.getStatus().name())){
					Thread.sleep(1000);
					retDroplet = dropletService.getDropletInfo(retDroplet.getId());
				}
				dropletFill.add(retDroplet);
			}
			generateServerConfigFile(dropletFill);

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
			LOGGER.info("All droplets info are {}", droplets.toString());
			break;

		case GET_ALL_PROPERTIES_OF_DIGITAL_OCEAN:
			Account account = dropletService.getAccountInfo();
			LOGGER.info("Account info {}", account.toString());
			Images images = dropletService.getAvailableImages(
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
			if (!droplet.isOff())
				return;
			LOGGER.info("Droplet is shutdown now start takeing snapshot.");
			if (dropletEntity.getSnapshotName() == null
					|| "".equalsIgnoreCase(dropletEntity.getSnapshotName())) {
				dropletService.takeDropletSnapshot(dropletId);
			} else {
				dropletService.takeDropletSnapshot(dropletId,
						dropletEntity.getSnapshotName());
			}
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

		default:
			LOGGER.info("No services is performed");
			break;

		}
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
		for (Droplet retDroplet : droplets) {
			String ipv4Address = retDroplet.getNetworks().getVersion4Networks().get(0).getIpAddress();
			ipv4Addrs.add(PRIFIX + (index++) + SUFFIX + ipv4Address);
		}
		try {
			CommonUtil.writeLine("serverConfigFile.properties", ipv4Addrs);
			LOGGER.info("serverConfigFile.properties file is create successfully");
		} catch (IOException e) {
			LOGGER.info("Unable to write the servers ips in serverConfigFile.properties file");
		}
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
