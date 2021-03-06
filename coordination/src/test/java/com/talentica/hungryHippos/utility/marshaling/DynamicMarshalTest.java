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
package com.talentica.hungryHippos.utility.marshaling;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.data.parser.CsvDataParser;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.domain.MutableDouble;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;


public class DynamicMarshalTest {
  private DynamicMarshal dynamicmarshal;
  private ByteBuffer bytebuffer;
  private FieldTypeArrayDataDescription dataDescription;

  @Before
  public void setUp() throws IOException {
    dataDescription = new FieldTypeArrayDataDescription(80);
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
  public void testreadvalue() throws IOException, InvalidRowException {
    CsvDataParser csvDataPreprocessor = new CsvDataParser(dataDescription);
    // Reader input = new
    // com.talentica.hungryHippos.utility.marshaling.FileReader("testSampleInput_1.txt");
    Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
        "src/test/resources/testSampleInputWithBlankLineAtEOF.txt", csvDataPreprocessor);
    Assert.assertNotNull(input);
    int noOfLines = 0;
    while (true) {
      List<Object> keylist = new ArrayList<Object>();
      DataTypes[] parts = input.read();
      if (parts == null) {
        break;
      }
      Assert.assertNotNull(parts);

      MutableCharArrayString key1 = (MutableCharArrayString) parts[0];
      MutableCharArrayString key2 = (MutableCharArrayString) parts[1];
      MutableCharArrayString key3 = (MutableCharArrayString) parts[2];
      MutableCharArrayString key4 = (MutableCharArrayString) parts[3];
      MutableCharArrayString key5 = (MutableCharArrayString) parts[4];
      MutableCharArrayString key6 = (MutableCharArrayString) parts[5];
      MutableDouble key7 = (MutableDouble)parts[6];
      MutableDouble key8 = (MutableDouble)parts[7];
      MutableCharArrayString key9 = (MutableCharArrayString) parts[8];

      keylist.add(key1.clone());
      keylist.add(key2.clone());
      keylist.add(key3.clone());
      keylist.add(key4.clone());
      keylist.add(key5.clone());
      keylist.add(key6.clone());
      keylist.add(key7.toDouble());
      keylist.add(key8.toDouble());
      keylist.add(key9.clone());


      dynamicmarshal.writeValue(0, key1, bytebuffer);
      dynamicmarshal.writeValue(1, key2, bytebuffer);
      dynamicmarshal.writeValue(2, key3, bytebuffer);
      dynamicmarshal.writeValue(3, key4, bytebuffer);
      dynamicmarshal.writeValue(4, key5, bytebuffer);
      dynamicmarshal.writeValue(5, key6, bytebuffer);
      dynamicmarshal.writeValue(6,key7, bytebuffer);
      dynamicmarshal.writeValue(7, key8, bytebuffer);
      dynamicmarshal.writeValue(8, key9, bytebuffer);


      for (int i = 0; i < 9; i++) {
        Object valueAtPosition = dynamicmarshal.readValue(i, bytebuffer);
        if(valueAtPosition instanceof MutableDouble){
          Assert.assertNotNull(valueAtPosition);
          Assert.assertEquals(keylist.get(i), ((MutableDouble) valueAtPosition).toDouble());
          }else{
            Assert.assertNotNull(valueAtPosition);
            Assert.assertEquals(keylist.get(i), valueAtPosition);
          }
      }
      noOfLines++;
    }

    Assert.assertEquals(999993, noOfLines);
  }


  @Test
  public void testreadvalueWithBlankLines() throws IOException, InvalidRowException {
    CsvDataParser csvDataPreprocessor = new CsvDataParser(dataDescription);
    Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
        "src/test/resources/testSampleInputWithBlankLines.txt", csvDataPreprocessor);
    Assert.assertNotNull(input);
    int noOfLines = 0;
    while (true) {
      List<Object> keylist = new ArrayList<Object>();
      DataTypes[] parts = input.read();
      if (parts == null) {
        break;
      }
      Assert.assertNotNull(parts);

      MutableCharArrayString key1 = (MutableCharArrayString) parts[0];
      MutableCharArrayString key2 = (MutableCharArrayString) parts[1];
      MutableCharArrayString key3 = (MutableCharArrayString) parts[2];
      MutableCharArrayString key4 = (MutableCharArrayString) parts[3];
      MutableCharArrayString key5 = (MutableCharArrayString) parts[4];
      MutableCharArrayString key6 = (MutableCharArrayString) parts[5];
      MutableDouble key7 = ((MutableDouble)parts[6]);
      MutableDouble key8 = ((MutableDouble)parts[7]);
      MutableCharArrayString key9 = (MutableCharArrayString) parts[8];

      keylist.add(key1.clone());
      keylist.add(key2.clone());
      keylist.add(key3.clone());
      keylist.add(key4.clone());
      keylist.add(key5.clone());
      keylist.add(key6.clone());
      keylist.add(key7.toDouble());
      keylist.add(key8.toDouble());
      keylist.add(key9.clone());


      dynamicmarshal.writeValue(0, key1, bytebuffer);
      dynamicmarshal.writeValue(1, key2, bytebuffer);
      dynamicmarshal.writeValue(2, key3, bytebuffer);
      dynamicmarshal.writeValue(3, key4, bytebuffer);
      dynamicmarshal.writeValue(4, key5, bytebuffer);
      dynamicmarshal.writeValue(5, key6, bytebuffer);
      dynamicmarshal.writeValue(6, key7, bytebuffer);
      dynamicmarshal.writeValue(7, key8, bytebuffer);
      dynamicmarshal.writeValue(8, key9, bytebuffer);

     
      for (int i = 0; i < 9; i++) {
        Object valueAtPosition = dynamicmarshal.readValue(i, bytebuffer);
        if(valueAtPosition instanceof MutableDouble){
        Assert.assertNotNull(valueAtPosition);
        Assert.assertEquals(keylist.get(i), ((MutableDouble) valueAtPosition).toDouble());
        }else{
          Assert.assertNotNull(valueAtPosition);
          Assert.assertEquals(keylist.get(i), valueAtPosition);
        }
      }
      noOfLines++;
    }

    Assert.assertEquals(4, noOfLines);
  }

  @Test
  public void testreadvalueWithBlankFile() throws IOException, InvalidRowException {
    CsvDataParser csvDataPreprocessor = new CsvDataParser(dataDescription);
    Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
        "src/test/resources/testSampleInput_2.txt", csvDataPreprocessor);
    Assert.assertNotNull(input);
    int noOfLines = 0;
    while (true) {
      List<Object> keylist = new ArrayList<Object>();
      DataTypes[] parts = input.read();
      if (parts == null) {
        break;
      }
      Assert.assertNotNull(parts);

      MutableCharArrayString key1 = (MutableCharArrayString) parts[0];
      MutableCharArrayString key2 = (MutableCharArrayString) parts[1];
      MutableCharArrayString key3 = (MutableCharArrayString) parts[2];
      MutableCharArrayString key4 = (MutableCharArrayString) parts[3];
      MutableCharArrayString key5 = (MutableCharArrayString) parts[4];
      MutableCharArrayString key6 = (MutableCharArrayString) parts[5];
      double key7 = Double.parseDouble(parts[6].toString());
      double key8 = Double.parseDouble(parts[7].toString());
      MutableCharArrayString key9 = (MutableCharArrayString) parts[8];

      keylist.add(key1.clone());
      keylist.add(key2.clone());
      keylist.add(key3.clone());
      keylist.add(key4.clone());
      keylist.add(key5.clone());
      keylist.add(key6.clone());
      keylist.add(key7);
      keylist.add(key8);
      keylist.add(key9.clone());


      dynamicmarshal.writeValue(0, key1, bytebuffer);
      dynamicmarshal.writeValue(1, key2, bytebuffer);
      dynamicmarshal.writeValue(2, key3, bytebuffer);
      dynamicmarshal.writeValue(3, key4, bytebuffer);
      dynamicmarshal.writeValue(4, key5, bytebuffer);
      dynamicmarshal.writeValue(5, key6, bytebuffer);
      dynamicmarshal.writeValue(6, key7, bytebuffer);
      dynamicmarshal.writeValue(7, key8, bytebuffer);
      dynamicmarshal.writeValue(8, key9, bytebuffer);


      for (int i = 0; i < 9; i++) {
        Object valueAtPosition = dynamicmarshal.readValue(i, bytebuffer);
        Assert.assertNotNull(valueAtPosition);
        Assert.assertEquals(keylist.get(i), valueAtPosition);
      }
      noOfLines++;
    }

    Assert.assertEquals(0, noOfLines);
  }

}

