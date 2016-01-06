/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author nitink
 *
 */
public class PropertyTest{
	
	@Test
	public void testGetProperties() {
		Properties properties = Property.getProperties();
		Assert.assertNotNull(properties);
	}
	
}
