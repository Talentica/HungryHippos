/**
 * 
 */
package com.talentica.hungryHippos.coordination.filter;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.InvalidRowExeption;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
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

	private static String jobuuid = "ABCSD12";
	private static String sampleBadRecordFile;
	private static String dataParserClassName;
	private static DataParser dataParser;
	private static Reader data;
	private static String badRecordsFile ;

	@Before
	public void setUp() {
		CommonUtil.loadDefaultPath(jobuuid);
		Property.initialize(PROPERTIES_NAMESPACE.MASTER);
		dataParserClassName = "com.talentica.hungryHippos.coordination.utility.CsvDataParser";
		sampleBadRecordFile =new File("").getAbsolutePath() + File.separator +  "bad_records_data.csv";
		badRecordsFile = new File("").getAbsolutePath() + File.separator+"test.err";
	}

	@Test
	public void testFilterBadRecords() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException {
		CommonUtil.loadDefaultPath(jobuuid);
		dataParser = (DataParser) Class.forName(dataParserClassName)
				.newInstance();
		data = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
				sampleBadRecordFile, dataParser);
		int actualBadRecords = 0;
		int expectedBadRows = 2;
		int lineNo = 0;
		FileWriter.openFile(badRecordsFile);
		while (true) {
			MutableCharArrayString[] parts = null;
			try {
				parts = data.read();
			} catch (InvalidRowExeption e) {
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
