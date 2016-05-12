package com.talentica.hungryHippos.utility.marshaling;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;

@Ignore
public class DynamicMarshalTest {
	private DynamicMarshal dynamicmarshal;
	private ByteBuffer bytebuffer;
	private FieldTypeArrayDataDescription dataDescription;
	
	@Before
	public void setUp() throws IOException {
		dataDescription = new FieldTypeArrayDataDescription();
		dataDescription.addFieldType(DataLocator.DataType.STRING, 2);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 2);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 2);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);
		dataDescription.addFieldType(DataLocator.DataType.DOUBLE, 8);
		dataDescription.addFieldType(DataLocator.DataType.DOUBLE, 8);
		dataDescription.addFieldType(DataLocator.DataType.STRING, 4);

		dynamicmarshal = new DynamicMarshal(dataDescription);
		byte[] bytes = new byte[dataDescription.getSize()];
		bytebuffer = ByteBuffer.wrap(bytes);
		
	}


	@After
	public void tearDown() throws IOException {
		/*
		 * if (fileinputstream != null) { fileinputstream.close(); }
		 */
	}


	@Test
	public void testreadvalue() throws IOException {

		//Reader input = new com.talentica.hungryHippos.utility.marshaling.FileReader("testSampleInput_1.txt");
		Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
				"src/test/resources/testSampleInputWithBlankLineAtEOF.txt");
		Assert.assertNotNull(input);
		int noOfLines=0;
		while (true) {
			List<Object> keylist = new ArrayList<Object>(); 
			MutableCharArrayString[] parts = input.read();
			if (parts == null) {
				break;
			}
			Assert.assertNotNull(parts);
			
			MutableCharArrayString key1 = parts[0];
			MutableCharArrayString key2 = parts[1];
			MutableCharArrayString key3 = parts[2];
			MutableCharArrayString key4 = parts[3];
			MutableCharArrayString key5 = parts[4];
			MutableCharArrayString key6 = parts[5];
			double key7 = Double.parseDouble(parts[6].toString());
			double key8 = Double.parseDouble(parts[7].toString());
			MutableCharArrayString key9 = parts[8];
			
			keylist.add(key1.clone());
			keylist.add(key2.clone());
			keylist.add(key3.clone());
			keylist.add(key4.clone());
			keylist.add(key5.clone());
			keylist.add(key6.clone());
			keylist.add(key7);
			keylist.add(key8);
			keylist.add(key9.clone());
					

			dynamicmarshal.writeValueString(0, key1, bytebuffer);
			dynamicmarshal.writeValueString(1, key2, bytebuffer);
			dynamicmarshal.writeValueString(2, key3, bytebuffer);
			dynamicmarshal.writeValueString(3, key4, bytebuffer);
			dynamicmarshal.writeValueString(4, key5, bytebuffer);
			dynamicmarshal.writeValueString(5, key6, bytebuffer);
			dynamicmarshal.writeValueDouble(6, key7, bytebuffer);
			dynamicmarshal.writeValueDouble(7, key8, bytebuffer);
			dynamicmarshal.writeValueString(8, key9, bytebuffer);
			
			
			for (int i = 0; i < 9; i++) {
						Object valueAtPosition = dynamicmarshal.readValue(i, bytebuffer);
						Assert.assertNotNull(valueAtPosition);
						Assert.assertEquals(keylist.get(i), valueAtPosition);
					}
			noOfLines++;
				}
					
		Assert.assertEquals(999993, noOfLines);
		}
	
	
	@Test
	public void testreadvalueWithBlankLines() throws IOException {

		Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader("src/test/resources/testSampleInputWithBlankLines.txt");
		Assert.assertNotNull(input);
		int noOfLines=0;
		while (true) {
			List<Object> keylist = new ArrayList<Object>(); 
			MutableCharArrayString[] parts = input.read();
			if (parts == null) {
				break;
			}
			Assert.assertNotNull(parts);
			
			MutableCharArrayString key1 = parts[0];
			MutableCharArrayString key2 = parts[1];
			MutableCharArrayString key3 = parts[2];
			MutableCharArrayString key4 = parts[3];
			MutableCharArrayString key5 = parts[4];
			MutableCharArrayString key6 = parts[5];
			double key7 = Double.parseDouble(parts[6].toString());
			double key8 = Double.parseDouble(parts[7].toString());
			MutableCharArrayString key9 = parts[8];
			
			keylist.add(key1.clone());
			keylist.add(key2.clone());
			keylist.add(key3.clone());
			keylist.add(key4.clone());
			keylist.add(key5.clone());
			keylist.add(key6.clone());
			keylist.add(key7);
			keylist.add(key8);
			keylist.add(key9.clone());
					

			dynamicmarshal.writeValueString(0, key1, bytebuffer);
			dynamicmarshal.writeValueString(1, key2, bytebuffer);
			dynamicmarshal.writeValueString(2, key3, bytebuffer);
			dynamicmarshal.writeValueString(3, key4, bytebuffer);
			dynamicmarshal.writeValueString(4, key5, bytebuffer);
			dynamicmarshal.writeValueString(5, key6, bytebuffer);
			dynamicmarshal.writeValueDouble(6, key7, bytebuffer);
			dynamicmarshal.writeValueDouble(7, key8, bytebuffer);
			dynamicmarshal.writeValueString(8, key9, bytebuffer);
			
			
			for (int i = 0; i < 9; i++) {
						Object valueAtPosition = dynamicmarshal.readValue(i, bytebuffer);
						Assert.assertNotNull(valueAtPosition);
						Assert.assertEquals(keylist.get(i), valueAtPosition);
					}
			noOfLines++;
				}
					
			Assert.assertEquals(4,noOfLines);
		}
	
	@Test
	public void testreadvalueWithBlankFile() throws IOException {

		Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader("src/test/resources/testSampleInput_2.txt");
		Assert.assertNotNull(input);
		int noOfLines=0;
		while (true) {
			List<Object> keylist = new ArrayList<Object>(); 
			MutableCharArrayString[] parts = input.read();
			if (parts == null) {
				break;
			}
			Assert.assertNotNull(parts);
			
			MutableCharArrayString key1 = parts[0];
			MutableCharArrayString key2 = parts[1];
			MutableCharArrayString key3 = parts[2];
			MutableCharArrayString key4 = parts[3];
			MutableCharArrayString key5 = parts[4];
			MutableCharArrayString key6 = parts[5];
			double key7 = Double.parseDouble(parts[6].toString());
			double key8 = Double.parseDouble(parts[7].toString());
			MutableCharArrayString key9 = parts[8];
			
			keylist.add(key1.clone());
			keylist.add(key2.clone());
			keylist.add(key3.clone());
			keylist.add(key4.clone());
			keylist.add(key5.clone());
			keylist.add(key6.clone());
			keylist.add(key7);
			keylist.add(key8);
			keylist.add(key9.clone());
					

			dynamicmarshal.writeValueString(0, key1, bytebuffer);
			dynamicmarshal.writeValueString(1, key2, bytebuffer);
			dynamicmarshal.writeValueString(2, key3, bytebuffer);
			dynamicmarshal.writeValueString(3, key4, bytebuffer);
			dynamicmarshal.writeValueString(4, key5, bytebuffer);
			dynamicmarshal.writeValueString(5, key6, bytebuffer);
			dynamicmarshal.writeValueDouble(6, key7, bytebuffer);
			dynamicmarshal.writeValueDouble(7, key8, bytebuffer);
			dynamicmarshal.writeValueString(8, key9, bytebuffer);
			
			
			for (int i = 0; i < 9; i++) {
						Object valueAtPosition = dynamicmarshal.readValue(i, bytebuffer);
						Assert.assertNotNull(valueAtPosition);
						Assert.assertEquals(keylist.get(i), valueAtPosition);
					}
			noOfLines++;
				}
					
			Assert.assertEquals(0,noOfLines);
		}

	}

