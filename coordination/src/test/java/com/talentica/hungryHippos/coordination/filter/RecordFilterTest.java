/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
/**
 * 
 */
package com.talentica.hungryHippos.coordination.filter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
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

	private static final String[] INPUT_DATA_DESCRIPTION = new String[] { "STRING-1", "STRING-1", "STRING-1",
			"STRING-3", "STRING-3", "STRING-3", "DOUBLE", "DOUBLE", "STRING-5" };

	@Before
	public void setUp() {
		dataParserClassName = "com.talentica.hungryHippos.client.data.parser.CsvDataParser";
		sampleBadRecordFile = new File("").getAbsolutePath() + File.separator + "temp.csv";
		badRecordsFile = new File("").getAbsolutePath() + File.separator + "test.err";
	}

	@Test
	public void testFilterBadRecords() throws InstantiationException, IllegalAccessException, ClassNotFoundException,
			IOException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException,
			KeeperException, InterruptedException, JAXBException {
		DataParser dataParser = (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class)
				.newInstance(FieldTypeArrayDataDescription.createDataDescription(INPUT_DATA_DESCRIPTION, 150));
		data = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(sampleBadRecordFile,
				dataParser);
		int actualBadRecords = 0;
		int expectedBadRows = 2;
		int lineNo = 0;
		FileWriter fileWriter = new FileWriter(badRecordsFile);
		fileWriter.openFile();
		while (true) {
			DataTypes[] parts = null;
			try {
				parts = data.read();
			} catch (InvalidRowException e) {
			  fileWriter.flushData(lineNo++, e);
				actualBadRecords++;
				continue;
			}
			if (parts == null) {
				data.close();
				break;
			}
		}
		fileWriter.close();
		Assert.assertEquals(expectedBadRows, actualBadRecords);
	}

}
