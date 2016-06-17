/**
 * 
 */
package com.talentica.hungryHippos.coordination.filter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileWriter;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;

/**
 * @author pooshans
 *
 */

public class RecordFilterTest {

  private static String sampleBadRecordFile;
  private static String dataParserClassName;
  private static Reader data;
  private static String badRecordsFile;

  @Before
  public void setUp() {
    Property.initialize(PROPERTIES_NAMESPACE.MASTER);
    dataParserClassName = "com.talentica.hungryHippos.client.data.parser.CsvDataParser";
    sampleBadRecordFile = new File("").getAbsolutePath() + File.separator + "temp.csv";
    badRecordsFile = new File("").getAbsolutePath() + File.separator + "test.err";
  }


  @Test
  public void testFilterBadRecords() throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, IOException, IllegalArgumentException, InvocationTargetException,
      NoSuchMethodException, SecurityException {
    DataParser dataParser =
        (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class)
            .newInstance(CommonUtil.getConfiguredDataDescription());
    data = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
        sampleBadRecordFile, dataParser);
    int actualBadRecords = 0;
    int expectedBadRows = 2;
    int lineNo = 0;
    FileWriter.openFile(badRecordsFile);
    while (true) {
      DataTypes[] parts = null;
      try {
        parts = data.read();
      } catch (InvalidRowException e) {
        FileWriter.flushData(lineNo++, e);
        actualBadRecords++;
        continue;
      }
      if (parts == null) {
        data.close();
        break;
      }
    }
    Assert.assertEquals(expectedBadRows, actualBadRecords);
  }

}
