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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JaxbUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(JaxbUtil.class);

  /**
   * Marshalling supplied object to XML document by JAXB annotations and serializing it to String
   * 
   * @param obj object to be marshalled
   * @return serialized XML document
   * @throws MarshalException
   */
  public static String marshalToXml(Object obj) throws JAXBException {
    JAXBContext jc = JAXBContextFactory.createContext(new Class[] {obj.getClass()}, null);
    Marshaller marshaller = jc.createMarshaller();
    StringWriter stringwriter = new StringWriter();
    marshaller.marshal(obj, stringwriter);
    return stringwriter.toString();
  }

  /**
   * Marshalling supplied object to XML document by JAXB annotations and serializing it to String
   * 
   * @param obj object to be marshalled
   * @return serialized XML document
   * @throws MarshalException
   */
  public static String marshalToJson(Object obj) {
    try {
      JAXBContext jc = JAXBContextFactory.createContext(new Class[] {obj.getClass()}, null);
      Marshaller marshaller = jc.createMarshaller();
      StringWriter stringwriter = new StringWriter();
      marshaller.setProperty(MarshallerProperties.MEDIA_TYPE, "application/json");
      marshaller.marshal(obj, stringwriter);
      return stringwriter.toString();
    } catch (JAXBException exception) {
      throw new RuntimeException(exception);
    }
  }

  /**
   * Unmarshalling from XML document by JAXB annotations
   * 
   * @param xml xml document serialized as String
   * @param clazz Class to which shoud be object unmarshalled
   * @return serialized XML document
   * @throws JAXBException
   */
  @SuppressWarnings("unchecked")
  public static <T> T unmarshalFromXml(String xml, Class<T> clazz) throws JAXBException {
    JAXBContext jaxbContext = JAXBContextFactory.createContext(new Class[] {clazz}, null);
    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    return (T) unmarshaller.unmarshal(new StringReader(xml));
  }

  /**
   * Unmarshalling from XML document by JAXB annotations
   * 
   * @param xml xml document serialized as String
   * @param clazz Class to which shoud be object unmarshalled
   * @return serialized XML document
   * @throws JAXBException
   */
  @SuppressWarnings("unchecked")
  public static <T> T unmarshalFromJson(String json, Class<T> clazz) throws JAXBException {
    try {
      JAXBContext jaxbContext = JAXBContextFactory.createContext(new Class[] {clazz}, null);
      Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
      unmarshaller.setProperty(UnmarshallerProperties.MEDIA_TYPE, "application/json");
      return (T) unmarshaller.unmarshal(new StringReader(json));
    } catch (JAXBException exception) {
      LOGGER.warn("Error while parsing JSON content:-{}", json);
      throw exception;
    }
  }

  /**
   * Unmarshalling from XML document by JAXB annotations
   * 
   * @param xml xml document serialized as String
   * @param clazz Class to which shoud be object unmarshalled
   * @return serialized XML document
   * @throws JAXBException
   * @throws FileNotFoundException
   */
  @SuppressWarnings("unchecked")
  public static <T> T unmarshalFromFile(String filePath, Class<T> clazz)
      throws JAXBException, FileNotFoundException {
    JAXBContext jaxbContext = JAXBContextFactory.createContext(new Class[] {clazz}, null);
    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    if (filePath.endsWith(".json")) {
      unmarshaller.setProperty(UnmarshallerProperties.MEDIA_TYPE, "application/json");
    }
    return (T) unmarshaller.unmarshal(new FileReader(filePath));
  }

  /**
   * Unmarshals object from XML or JSON configuration content.
   * 
   * @param content
   * @param clazz
   * @return
   * @throws JAXBException
   * @throws FileNotFoundException
   */
  public static <T> T unmarshal(String content, Class<T> clazz)
      throws JAXBException, FileNotFoundException {
    T object = null;
    try {
      object = unmarshalFromXml(content, clazz);
    } catch (Exception exception) {
      object = unmarshalFromJson(content, clazz);
    }
    return object;
  }

}
