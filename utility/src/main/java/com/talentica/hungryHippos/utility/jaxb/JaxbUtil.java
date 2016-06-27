package com.talentica.hungryHippos.utility.jaxb;

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

public final class JaxbUtil {

	/**
	 * Marshalling supplied object to XML document by JAXB annotations and
	 * serializing it to String
	 * 
	 * @param obj
	 *            object to be marshalled
	 * @return serialized XML document
	 * @throws JAXBException
	 */
	public static String marshal(Object obj) throws JAXBException {
		StringWriter stringwriter = new StringWriter();
		JAXBContext jc = JAXBContext.newInstance(obj.getClass());
		Marshaller marshaller = jc.createMarshaller();
		marshaller.marshal(obj, stringwriter);
		return stringwriter.toString();
	}

	/**
	 * Unmarshalling from XML document by JAXB annotations
	 * 
	 * @param xml
	 *            xml document serialized as String
	 * @param clazz
	 *            Class to which shoud be object unmarshalled
	 * @return serialized XML document
	 * @throws JAXBException
	 */
	@SuppressWarnings("unchecked")
	public static <T> T unmarshal(String xml, Class<T> clazz) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance(clazz);
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		return (T) unmarshaller.unmarshal(new StringReader(xml));
	}

}