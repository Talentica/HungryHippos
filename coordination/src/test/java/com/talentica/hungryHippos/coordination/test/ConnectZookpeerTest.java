/**
 * 
 */
package com.talentica.hungryHippos.coordination.test;

import java.util.Properties;

import org.junit.Test;

import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;

/**
 * @author PooshanS
 *
 */
public class ConnectZookpeerTest {

	
	@Test
	public void testStartZk(){
		try {
			Property.overrideConfigurationProperties("D:\\HUNGRY_HIPPOS\\HungryHippos\\utility\\src\\main\\resources\\config.properties");
			Property.initialize(PROPERTIES_NAMESPACE.ZK);
			CommonUtil.connectZK();
			Properties properties = Property.getProperties();
			System.out.println(properties);
			Properties properties2 = Property.loadServerProperties();
			System.out.println(properties2);
			
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
}
