/**
 * 
 */
package com.talentica.hungryHippos.coordination.test;

import java.util.Properties;

import org.junit.Test;

import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.PropertyOld;
import com.talentica.hungryHippos.coordination.utility.PropertyOld.PROPERTIES_NAMESPACE;

/**
 * @author PooshanS
 *
 */
public class ConnectZookpeerTest {

	
	@Test
	public void testStartZk(){
		try {
			PropertyOld.overrideConfigurationProperties("D:\\HUNGRY_HIPPOS\\HungryHippos\\utility\\src\\main\\resources\\config.properties");
			PropertyOld.initialize(PROPERTIES_NAMESPACE.ZK);
			CommonUtil.connectZK();
			Properties properties = PropertyOld.getProperties();
			System.out.println(properties);
			Properties properties2 = PropertyOld.loadServerProperties();
			System.out.println(properties2);
			
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
}
