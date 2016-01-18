package com.talentica.hungryHippos.utility.marshaling;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileReaderTest {

	private FileReader fileReader;

	private FileReader fileReaderBlankLinesFile;

	private FileReader fileReaderBlankLineAtEofFile;

	@Before
	public void setUp() throws IOException {
		fileReader = new FileReader(new File("src/test/resources/testSampleInputWithNoBlankLineAtEOF.txt"));
		fileReader.setNumFields(9);
		fileReader.setMaxsize(25);
		fileReaderBlankLinesFile = new FileReader(new File("src/test/resources/testSampleInputWithBlankLines.txt"));
		fileReaderBlankLinesFile.setNumFields(9);
		fileReaderBlankLinesFile.setMaxsize(25);
		fileReaderBlankLineAtEofFile = new FileReader(new File("src/test/resources/testSampleInputWithBlankLines.txt"));
		fileReaderBlankLineAtEofFile.setNumFields(9);
		fileReaderBlankLineAtEofFile.setMaxsize(25);
	}

	@Test
	public void testRead() throws IOException {
		int numberOfLines = 0;
		while (true) {
			MutableCharArrayString[] data = fileReader.read();
			if (data == null) {
				break;
			}
			Assert.assertNotEquals(0, data.length);
			Assert.assertNotEquals(0, data[0].length());
			numberOfLines++;
		}
		Assert.assertEquals(2, numberOfLines);
	}

	@Test
	public void testReadWithNoBlankLineAtTheEndOfFile() throws IOException {
		int numberOfLines = 0;
		while (true) {
			MutableCharArrayString[] data = fileReaderBlankLineAtEofFile.read();
			if (data == null) {
				break;
			}
			Assert.assertNotEquals(0, data.length);
			Assert.assertNotEquals(0, data[0].length());
			numberOfLines++;
		}
		Assert.assertEquals(4, numberOfLines);
	}

	@Test
	public void testReadFromFileHavingBlankLines() throws IOException {
		int numberOfLines = 0;
		while (true) {
			MutableCharArrayString[] data = fileReaderBlankLinesFile.read();
			if (data == null) {
				break;
			}
			Assert.assertNotEquals(0, data.length);
			Assert.assertNotEquals(0, data[0].length());
			numberOfLines++;
		}
		Assert.assertEquals(4, numberOfLines);
	}

	@After
	public void tearDown() throws IOException {
		fileReader.close();
		fileReaderBlankLinesFile.close();
		fileReaderBlankLineAtEofFile.close();
	}

}
