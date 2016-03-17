/**
 * 
 */
package com.talentica.hungryHippos.droplet.entity;

import java.util.ArrayList;
import java.util.List;

import com.myjeeva.digitalocean.pojo.Droplet;
import com.myjeeva.digitalocean.pojo.RateLimitBase;

/**
 * @author PooshanS
 *
 */
public class DigitalOceanEntity extends RateLimitBase{
	
	private static final long serialVersionUID = -8701443487566205819L;

	private Droplet droplet;
	
	private String authToken;
	
	private String request;
	
	private String ids;

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
	
	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}
	
	public String getIds() {
		return ids;
	}

	public void setIds(String ids) {
		this.ids = ids;
	}
	
	public List<Integer> getIdsAsList(){
		if(ids == null) return null;
		List<Integer> idsAsList = new ArrayList<Integer>();
		for(String id : ids.split(",")){
			idsAsList.add(Integer.valueOf(id));
		}
		return idsAsList;
	}

	@Override
	public String toString() {
		return "DigitalOceanEntity [droplet=" + droplet + ", authToken="
				+ authToken + ", request=" + request + ", ids=" + ids + "]";
	}
	
}
