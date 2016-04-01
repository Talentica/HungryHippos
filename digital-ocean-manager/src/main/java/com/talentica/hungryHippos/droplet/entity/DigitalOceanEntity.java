/**
 * 
 */
package com.talentica.hungryHippos.droplet.entity;

import java.util.ArrayList;
import java.util.List;

import com.myjeeva.digitalocean.pojo.Droplet;
import com.myjeeva.digitalocean.pojo.Image;
import com.myjeeva.digitalocean.pojo.RateLimitBase;
import com.talentica.hungryHippos.droplet.util.ServiceRequestEnum;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanEntity extends RateLimitBase {

	private static final long serialVersionUID = -8701443487566205819L;

	private Droplet droplet;

	private String authToken;

	private ServiceRequestEnum request;

	private String ids;

	private int powerOffOnFlag;

	private int pageNo;

	private int perPage;

	private String snapshotName;
	
	private Image image;

	public Droplet getDroplet() {
		return droplet;
	}

	public void setDroplet(Droplet droplet) {
		this.droplet = droplet;
	}

	public String getAuthToken() {
		return authToken;
	}

	public void setAuthToken(String authToken) {
		this.authToken = authToken;
	}

	public ServiceRequestEnum getRequest() {
		return request;
	}

	public void setRequest(ServiceRequestEnum request) {
		this.request = request;
	}

	public String getIds() {
		return ids;
	}

	public void setIds(String ids) {
		this.ids = ids;
	}

	public int getPageNo() {
		return pageNo;
	}

	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}

	public int getPerPage() {
		return perPage;
	}

	public void setPerPage(int perPage) {
		this.perPage = perPage;
	}

	public int getPowerOffOnFlag() {
		return powerOffOnFlag;
	}

	public String getSnapshotName() {
		return snapshotName;
	}

	public void setSnapshotName(String snapshotName) {
		this.snapshotName = snapshotName;
	}

	public void setPowerOffOnFlag(int powerOffOnFlag) {
		this.powerOffOnFlag = powerOffOnFlag;
	}

	public Image getImage() {
		return image;
	}

	public void setImage(Image image) {
		this.image = image;
	}

	public List<Integer> getIdsAsList() {
		if (ids == null)
			return null;
		List<Integer> idsAsList = new ArrayList<Integer>();
		for (String id : ids.split(",")) {
			idsAsList.add(Integer.valueOf(id.trim()));
		}
		return idsAsList;
	}

	@Override
	public String toString() {
		return "DigitalOceanEntity [droplet=" + droplet + ", authToken="
				+ authToken + ", request=" + request + ", ids=" + ids
				+ ", powerOffOnFlag=" + powerOffOnFlag + ", pageNo=" + pageNo
				+ ", perPage=" + perPage + ", snapshotName=" + snapshotName
				+ "]";
	}

}
