package com.talentica.hungryHippos.utility.marshaling;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileReaderTest {

	private FileReader fileReader;

	@Before
	public void setUp() throws IOException {
		fileReader = new FileReader(new File("src/test/resources/testSampleInput.txt"));
		fileReader.setNumFields(9);
		fileReader.setMaxsize(25);
	}

	@Test
	public void testRead() throws IOException {
		int numberOfLines = 0;
		while (true) {
			MutableCharArrayString[] data = fileReader.read();
			if (data == null) {
				break;
			}
			numberOfLines++;
		}
		Assert.assertEquals(2, numberOfLines);
	}

}
