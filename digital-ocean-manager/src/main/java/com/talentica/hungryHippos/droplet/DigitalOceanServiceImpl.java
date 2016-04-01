/**
 * 
 */
package com.talentica.hungryHippos.droplet;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myjeeva.digitalocean.common.ActionType;
import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.myjeeva.digitalocean.impl.DigitalOceanClient;
import com.myjeeva.digitalocean.pojo.Account;
import com.myjeeva.digitalocean.pojo.Action;
import com.myjeeva.digitalocean.pojo.Actions;
import com.myjeeva.digitalocean.pojo.Backups;
import com.myjeeva.digitalocean.pojo.Delete;
import com.myjeeva.digitalocean.pojo.Domain;
import com.myjeeva.digitalocean.pojo.DomainRecord;
import com.myjeeva.digitalocean.pojo.DomainRecords;
import com.myjeeva.digitalocean.pojo.Domains;
import com.myjeeva.digitalocean.pojo.Droplet;
import com.myjeeva.digitalocean.pojo.Droplets;
import com.myjeeva.digitalocean.pojo.FloatingIP;
import com.myjeeva.digitalocean.pojo.FloatingIPs;
import com.myjeeva.digitalocean.pojo.Image;
import com.myjeeva.digitalocean.pojo.Images;
import com.myjeeva.digitalocean.pojo.Kernels;
import com.myjeeva.digitalocean.pojo.Key;
import com.myjeeva.digitalocean.pojo.Keys;
import com.myjeeva.digitalocean.pojo.Neighbors;
import com.myjeeva.digitalocean.pojo.Regions;
import com.myjeeva.digitalocean.pojo.Sizes;
import com.myjeeva.digitalocean.pojo.Snapshots;
import com.talentica.hungryHippos.droplet.main.DigitalOceanManager;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanServiceImpl implements DigitalOceanService{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DigitalOceanServiceImpl.class);
	private DigitalOceanClient digitalOceanClient;
	
	public DigitalOceanServiceImpl(String authToken){
		digitalOceanClient = new DigitalOceanClient(authToken);
	}
	
	@Override
	public Droplet createDroplet(Droplet droplet) throws RequestUnsuccessfulException, DigitalOceanException {
		return digitalOceanClient.createDroplet(droplet);
		
	}

	@Override
	public Keys getAvailableKeys(Integer pageNo) throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableKeys(pageNo);
	}

	@Override
	public Droplets getAvailableDroplets(Integer pageNo, Integer perPage)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableDroplets(pageNo, perPage);
	}

	@Override
	public Kernels getAvailableKernels(Integer dropletId, Integer pageNo,
			Integer perPage) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableKernels(dropletId, pageNo, perPage);
	}

	@Override
	public Snapshots getAvailableSnapshots(Integer dropletId, Integer pageNo,
			Integer perPage) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableSnapshots(dropletId, pageNo, perPage);
	}

	@Override
	public Backups getAvailableBackups(Integer dropletId, Integer pageNo)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableBackups(dropletId, pageNo);
	}

	@Override
	public Droplet getDropletInfo(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getDropletInfo(dropletId);
	}

	@Override
	public Droplets createDroplets(Droplet droplet)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.createDroplets(droplet);
	}

	@Override
	public Delete deleteDroplet(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.deleteDroplet(dropletId);
	}

	@Override
	public Droplets getDropletNeighbors(Integer dropletId, Integer pageNo)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getDropletNeighbors(dropletId, pageNo);
	}

	@Override
	public Neighbors getAllDropletNeighbors(Integer pageNo)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getAllDropletNeighbors(pageNo);
	}

	@Override
	public Action rebootDroplet(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.rebootDroplet(dropletId);
	}

	@Override
	public Action powerCycleDroplet(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.powerCycleDroplet(dropletId);
	}

	@Override
	public Action shutdownDroplet(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.shutdownDroplet(dropletId);
	}

	@Override
	public Action powerOffDroplet(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.powerOffDroplet(dropletId);
	}

	@Override
	public Action powerOnDroplet(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.powerOnDroplet(dropletId);
	}

	@Override
	public Action resetDropletPassword(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.resetDropletPassword(dropletId);
	}

	@Override
	public Action resizeDroplet(Integer dropletId, String size)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.resizeDroplet(dropletId, size);
	}

	@Override
	public Action takeDropletSnapshot(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.takeDropletSnapshot(dropletId);
	}

	@Override
	public Action takeDropletSnapshot(Integer dropletId, String snapshotName)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.takeDropletSnapshot(dropletId, snapshotName);
	}

	@Override
	public Action restoreDroplet(Integer dropletId, Integer imageId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.restoreDroplet(dropletId, imageId);
	}

	@Override
	public Action rebuildDroplet(Integer dropletId, Integer imageId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.rebuildDroplet(dropletId, imageId);
	}

	@Override
	public Action enableDropletBackups(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.enableDropletBackups(dropletId);
	}

	@Override
	public Action disableDropletBackups(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.disableDropletBackups(dropletId);
	}

	@Override
	public Action renameDroplet(Integer dropletId, String name)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.renameDroplet(dropletId, name);
	}

	@Override
	public Action changeDropletKernel(Integer dropletId, Integer kernelId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.changeDropletKernel(dropletId, kernelId);
	}

	@Override
	public Action enableDropletIpv6(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.enableDropletIpv6(dropletId);
	}

	@Override
	public Action enableDropletPrivateNetworking(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.enableDropletPrivateNetworking(dropletId);
	}

	@Override
	public Account getAccountInfo() throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getAccountInfo();
	}

	@Override
	public Actions getAvailableActions(Integer pageNo, Integer perPage)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableActions(pageNo, perPage);
	}

	@Override
	public Action getActionInfo(Integer actionId) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getActionInfo(actionId);
	}

	@Override
	public Actions getAvailableDropletActions(Integer dropletId,
			Integer pageNo, Integer perPage) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableDropletActions(dropletId, pageNo, perPage);
	}

	@Override
	public Actions getAvailableImageActions(Integer imageId, Integer pageNo,
			Integer perPage) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableImageActions(imageId, pageNo, perPage);
	}

	@Override
	public Actions getAvailableFloatingIPActions(String ipAddress,
			Integer pageNo, Integer perPage) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableFloatingIPActions(ipAddress, pageNo, perPage);
	}

	@Override
	public Action getFloatingIPActionInfo(String ipAddress, Integer actionId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getFloatingIPActionInfo(ipAddress, actionId);
	}

	@Override
	public Images getAvailableImages(Integer pageNo, Integer perPage)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableImages(pageNo, perPage);
	}

	@Override
	public Images getAvailableImages(Integer pageNo, Integer perPage,
			ActionType type) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableImages(pageNo, perPage, type);
	}

	@Override
	public Images getUserImages(Integer pageNo, Integer perPage)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getUserImages(pageNo, perPage);
	}

	@Override
	public Image getImageInfo(Integer imageId) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getImageInfo(imageId);
	}

	@Override
	public Image getImageInfo(String slug) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getImageInfo(slug);
	}

	@Override
	public Image updateImage(Image image) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.updateImage(image);
	}

	@Override
	public Delete deleteImage(Integer imageId) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.deleteImage(imageId);
	}

	@Override
	public Action transferImage(Integer imageId, String regionSlug)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.transferImage(imageId, regionSlug);
	}

	@Override
	public Action convertImage(Integer imageId) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.convertImage(imageId);
	}

	@Override
	public Regions getAvailableRegions(Integer pageNo)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableRegions(pageNo);
	}

	@Override
	public Sizes getAvailableSizes(Integer pageNo)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableSizes(pageNo);
	}

	@Override
	public Domains getAvailableDomains(Integer pageNo)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableDomains(pageNo);
	}

	@Override
	public Domain getDomainInfo(String domainName)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getDomainInfo(domainName);
	}

	@Override
	public Domain createDomain(Domain domain) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.createDomain(domain);
	}

	@Override
	public Delete deleteDomain(String domainName) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.deleteDomain(domainName);
	}

	@Override
	public DomainRecords getDomainRecords(String domainName)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getDomainRecords(domainName);
	}

	@Override
	public DomainRecord createDomainRecord(String domainName,
			DomainRecord domainRecord) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.createDomainRecord(domainName, domainRecord);
	}

	@Override
	public DomainRecord getDomainRecordInfo(String domainName, Integer recordId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getDomainRecordInfo(domainName, recordId);
	}

	@Override
	public DomainRecord updateDomainRecord(String domainName, Integer recordId,
			DomainRecord domainRecord) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.updateDomainRecord(domainName, recordId, domainRecord);
	}

	@Override
	public Delete deleteDomainRecord(String domainName, Integer recordId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.deleteDomainRecord(domainName, recordId);
	}

	@Override
	public Key getKeyInfo(Integer sshKeyId) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getKeyInfo(sshKeyId);
	}

	@Override
	public Key getKeyInfo(String fingerprint) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.getKeyInfo(fingerprint);
	}

	@Override
	public Key createKey(Key newKey) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.createKey(newKey);
	}

	@Override
	public Key updateKey(Integer sshKeyId, String newSshKeyName)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.updateKey(sshKeyId, newSshKeyName);
	}

	@Override
	public Key updateKey(String fingerprint, String newSshKeyName)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.updateKey(fingerprint, newSshKeyName);
	}

	@Override
	public Delete deleteKey(Integer sshKeyId) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.deleteKey(sshKeyId);
	}

	@Override
	public Delete deleteKey(String fingerprint) throws DigitalOceanException,
			RequestUnsuccessfulException {
		return digitalOceanClient.deleteKey(fingerprint);
	}

	@Override
	public FloatingIPs getAvailableFloatingIPs(Integer pageNo, Integer perPage)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getAvailableFloatingIPs(pageNo, perPage);
	}

	@Override
	public FloatingIP createFloatingIP(Integer dropletId)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.createFloatingIP(dropletId);
	}

	@Override
	public FloatingIP createFloatingIP(String region)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.createFloatingIP(region);
	}

	@Override
	public FloatingIP getFloatingIPInfo(String ipAddress)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.getFloatingIPInfo(ipAddress);
	}

	@Override
	public Delete deleteFloatingIP(String ipAddress)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.deleteFloatingIP(ipAddress);
	}

	@Override
	public Action assignFloatingIP(Integer dropletId, String ipAddress)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.assignFloatingIP(dropletId, ipAddress);
	}

	@Override
	public Action unassignFloatingIP(String ipAddress)
			throws DigitalOceanException, RequestUnsuccessfulException {
		return digitalOceanClient.unassignFloatingIP(ipAddress);
	}

	@Override
	public List<Delete> deleteDroplets(List<Integer> dropletIdList) throws DigitalOceanException, RequestUnsuccessfulException {
		List<Delete> deleteLists = new ArrayList<Delete>(); 
		for(Integer dropletId : dropletIdList){
			try{
			deleteLists.add(deleteDroplet(dropletId));
			}catch(DigitalOceanException ex){
				LOGGER.info("Unable to delete the droplet id {}",dropletId);
			}
		}
		return deleteLists;
	}

	
}
