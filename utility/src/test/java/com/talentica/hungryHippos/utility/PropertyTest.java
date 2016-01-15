/**
 * 
 */
package com.talentica.hungryHippos.utility;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author nitink
 *
 */
public class PropertyTest{
	
	@Test
	public void testGetTotalNumberOfNodes() {
		int totalNoOfNodes = Property.getTotalNumberOfNodes();
		Assert.assertEquals(5, totalNoOfNodes);
	}
	
}
