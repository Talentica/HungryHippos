/**
 * 
 */
package com.talentica.hungryHippos.droplet;

import java.util.List;

import com.myjeeva.digitalocean.DigitalOcean;
import com.myjeeva.digitalocean.exception.DigitalOceanException;
import com.myjeeva.digitalocean.exception.RequestUnsuccessfulException;
import com.myjeeva.digitalocean.pojo.Delete;

/**
 * @author PooshanS
 *
 */
public interface DigitalOceanDroplet extends DigitalOcean{
	
	public List<Delete> deleteDroplets(List<Integer> dropletIdList) throws DigitalOceanException, RequestUnsuccessfulException;
}
