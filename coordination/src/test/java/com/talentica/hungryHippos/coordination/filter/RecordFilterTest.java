/**
 * 
 */
package com.talentica.hungryHippos.coordination.filter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.coordination.utility.Property;
import com.talentica.hungryHippos.coordination.utility.Property.PROPERTIES_NAMESPACE;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileWriter;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;

/**
 * @author pooshans
 *
 */

public class RecordFilterTest {

  private static String dataParserClassName;
  private static Reader data;
  private static String badRecordsFile;

  @Before
  public void setUp() {
    Property.initialize(PROPERTIES_NAMESPACE.MASTER);
    dataParserClassName = "com.talentica.hungryHippos.client.data.parser.CsvDataParser";
    badRecordsFile = new File("").getAbsolutePath() + File.separator + "test.err";
    badRecordsFile = new File("").getAbsolutePath() + File.separator + "test.err";
  }

  @Ignore
  @Test
  public void testFilterBadRecords()
      throws IllegalAccessException, ClassNotFoundException, IOException, IllegalArgumentException,
      InvocationTargetException, NoSuchMethodException, SecurityException, InstantiationException {
    int actualBadRecords = 0;
    int expectedBadRows = 1;
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
