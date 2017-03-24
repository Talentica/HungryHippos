/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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