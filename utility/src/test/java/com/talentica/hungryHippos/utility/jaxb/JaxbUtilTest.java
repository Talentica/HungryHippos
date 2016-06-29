package com.talentica.hungryHippos.utility.jaxb;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Test;

public final class JaxbUtilTest {

	@Test
	public void testMarshalToXml() throws JAXBException {
		JaxbTest testObj = new JaxbTest();
		testObj.setTestA("A");
		testObj.setTestB("B");
		testObj.setTestC("C");
		String xmlString = JaxbUtil.marshalToXml(testObj);
		System.out.println(xmlString);
		Assert.assertNotNull(xmlString);
		Assert.assertEquals(
				"<?xml version=\"1.0\" encoding=\"UTF-8\"?><jaxbTest><testA>A</testA><testB>B</testB></jaxbTest>",
				xmlString);
	}

	@Test
	public void testMarshalToJson() throws JAXBException {
		JaxbTest testObj = new JaxbTest();
		testObj.setTestA("A");
		testObj.setTestB("B");
		testObj.setTestC("C");
		String jsonString = JaxbUtil.marshalToJson(testObj);
		System.out.println(jsonString);
		Assert.assertNotNull(jsonString);
		Assert.assertEquals("{\"jaxbTest\":{\"testA\":\"A\",\"testB\":\"B\"}}", jsonString);
	}

	@Test
	public void testUnmarshalFromXml() throws JAXBException {
		JaxbTest actualObj = JaxbUtil.unmarshalFromXml(
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><jaxbTest><testA>A</testA><testB>B</testB><testC>C</testC></jaxbTest>",
				JaxbTest.class);
		Assert.assertEquals("A", actualObj.getTestA());
		Assert.assertEquals("B", actualObj.getTestB());
		Assert.assertNull(actualObj.getTestC());
	}

	@Test
	public void testUnmarshalFromJson() throws JAXBException {
		JaxbTest actualObj = JaxbUtil.unmarshalFromJson("{\"jaxbTest\":{\"testA\":\"A\",\"testB\":\"B\"}}",
				JaxbTest.class);
		Assert.assertEquals("A", actualObj.getTestA());
		Assert.assertEquals("B", actualObj.getTestB());
		Assert.assertNull(actualObj.getTestC());
	}

	@Test
	public void testUnmarshalFromJsonFile() throws FileNotFoundException, JAXBException {
		String sampleJsonFilePath = getClass().getClassLoader().getResource("sampleJson.json").getPath();
		JaxbTest actualObj = JaxbUtil.unmarshalFromFile(sampleJsonFilePath, JaxbTest.class);
		Assert.assertEquals("A", actualObj.getTestA());
		Assert.assertEquals("B", actualObj.getTestB());
		Assert.assertNull(actualObj.getTestC());
	}

	@Test
	public void testUnmarshalFromXmlFile() throws FileNotFoundException, JAXBException {
		String sampleJsonFilePath = getClass().getClassLoader().getResource("sampleXml.xml").getPath();
		JaxbTest actualObj = JaxbUtil.unmarshalFromFile(sampleJsonFilePath, JaxbTest.class);
		Assert.assertEquals("A", actualObj.getTestA());
		Assert.assertEquals("B", actualObj.getTestB());
		Assert.assertNull(actualObj.getTestC());
	}

}