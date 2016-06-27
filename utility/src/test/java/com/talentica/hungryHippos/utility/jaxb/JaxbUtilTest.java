package com.talentica.hungryHippos.utility.jaxb;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Test;


public final class JaxbUtilTest {
	
	@Test
	public void testMarshal() throws JAXBException {
		JaxbTest testObj= new JaxbTest();
		testObj.setTestA("A");
		testObj.setTestB("B");
		testObj.setTestC("C");
		String xmlString = JaxbUtil.marshal(testObj);
		System.out.println(xmlString);
		Assert.assertNotNull(xmlString);
		Assert.assertEquals(
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><jaxbTest><testA>A</testA><testB>B</testB></jaxbTest>",
				xmlString);
	}

	@Test
	public void testUnmarshal() throws JAXBException {
		JaxbTest actualObj = JaxbUtil.unmarshal(
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><jaxbTest><testA>A</testA><testB>B</testB><testC>C</testC></jaxbTest>",
				JaxbTest.class);
		Assert.assertEquals("A", actualObj.getTestA());
		Assert.assertEquals("B", actualObj.getTestB());
		Assert.assertNull(actualObj.getTestC());
	}

}