/**
 * 
 */
package com.talentica.hungryHippos.droplet.util;

/**
 * @author PooshanS
 *
 */
public enum ServiceRequestEnum {

	CREATE("CREATE"), DELETE("DELETE"), RENAME("RENAME"), POWER_OFF_ON(
			"POWER_OFF_ON"), GET_ALL_DROPLET_INFO("GET_ALL_DROPLET_INFO"), GET_ALL_PROPERTIES_OF_DIGITAL_OCEAN(
			"GET_ALL_PROPERTIES_OF_DIGITAL_OCEAN"),SNAPSHOT("SNAPSHOT"),SHUTDOWN("SHUTDOWN"),CREATE_KEY("CREATE_KEY");

	private String request;

	ServiceRequestEnum(String request) {
		this.request = request;
	}

	public String getRequest() {
		return request;
	}

}
