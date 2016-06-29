package com.talentica.hungryHippos.utility.jaxb;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.MarshalException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.jaxb.UnmarshallerProperties;

public final class JaxbUtil {

	/**
	 * Marshalling supplied object to XML document by JAXB annotations and
	 * serializing it to String
	 * 
	 * @param obj
	 *            object to be marshalled
	 * @return serialized XML document
	 * @throws MarshalException
	 */
	public static String marshalToXml(Object obj) throws JAXBException {
		JAXBContext jc = JAXBContextFactory.createContext(new Class[] { obj.getClass() }, null);
		Marshaller marshaller = jc.createMarshaller();
		StringWriter stringwriter = new StringWriter();
		marshaller.marshal(obj, stringwriter);
		return stringwriter.toString();
	}

	/**
	 * Marshalling supplied object to XML document by JAXB annotations and
	 * serializing it to String
	 * 
	 * @param obj
	 *            object to be marshalled
	 * @return serialized XML document
	 * @throws MarshalException
	 */
	public static String marshalToJson(Object obj) throws JAXBException {
		JAXBContext jc = JAXBContextFactory.createContext(new Class[] { obj.getClass() }, null);
		Marshaller marshaller = jc.createMarshaller();
		StringWriter stringwriter = new StringWriter();
		marshaller.setProperty(MarshallerProperties.MEDIA_TYPE, "application/json");
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
	public static <T> T unmarshalFromXml(String xml, Class<T> clazz) throws JAXBException {
		JAXBContext jaxbContext = JAXBContextFactory.createContext(new Class[] { clazz }, null);
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		return (T) unmarshaller.unmarshal(new StringReader(xml));
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
	public static <T> T unmarshalFromJson(String json, Class<T> clazz) throws JAXBException {
		JAXBContext jaxbContext = JAXBContextFactory.createContext(new Class[] { clazz }, null);
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		unmarshaller.setProperty(UnmarshallerProperties.MEDIA_TYPE, "application/json");
		return (T) unmarshaller.unmarshal(new StringReader(json));
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
	 * @throws FileNotFoundException
	 */
	@SuppressWarnings("unchecked")
	public static <T> T unmarshalFromFile(String filePath, Class<T> clazz) throws JAXBException, FileNotFoundException {
		JAXBContext jaxbContext = JAXBContextFactory.createContext(new Class[] { clazz }, null);
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		if (filePath.endsWith(".json")) {
			unmarshaller.setProperty(UnmarshallerProperties.MEDIA_TYPE, "application/json");
		}
		return (T) unmarshaller.unmarshal(new FileReader(filePath));
	}

}