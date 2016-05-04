package com.talentica.hungryHippos.droplet.util;

import org.junit.Assert;
import org.junit.Test;


public class DigitalOceanServiceUtilTest {

	@Test
	public void testGetColumnsConfiguration() {
		String columnConfiguration = DigitalOceanServiceUtil
				.getColumnsConfiguration("STRING-3,STRING-3,STRING-2,DOUBLE-0,DOUBLE-3,STRING-5");
		Assert.assertEquals("key1,key2,key3,key4,key5,key6", columnConfiguration);
	}

}
