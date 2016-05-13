package com.talentica.hungryHippos.utility.marshaling;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileReader;

public class FileReaderTest {

	private FileReader fileReader;

	private FileReader fileReaderBlankLinesFile;

	private FileReader fileReaderBlankLineAtEofFile;

	private FileReader fileReaderWithBlankLineAtEOF;

	private FileReader testSampleFileGeneratedOnWindows;

	@Before
	public void setUp() throws IOException {
		ClassLoader classLoader = this.getClass().getClassLoader();
		DataDescription dataDescription = FieldTypeArrayDataDescription.createDataDescription(
				"STRING-1,STRING-1,STRING-1,STRING-1,DOUBLE-0,DOUBLE-0,DOUBLE-0,DOUBLE-0,STRING-3".split(","));
		fileReader = new FileReader(
				new File(classLoader.getResource("testSampleInputWithNoBlankLineAtEOF.txt").getPath()),
				dataDescription);
		fileReaderBlankLinesFile = new FileReader(
				new File(classLoader.getResource("testSampleInputWithBlankLines.txt").getPath()), dataDescription);
		fileReaderBlankLineAtEofFile = new FileReader(
				new File(classLoader.getResource("testSampleInputWithBlankLines.txt").getPath()), dataDescription);
		fileReaderWithBlankLineAtEOF = new FileReader(
				new File(classLoader.getResource("testSampleInputWithBlankLineAtEOF.txt").getPath()), dataDescription);
		DataDescription dataDescriptionWindowsTestFile = FieldTypeArrayDataDescription
				.createDataDescription("STRING-3,LONG-0".split(","));
		testSampleFileGeneratedOnWindows = new FileReader(
				new File(classLoader.getResource("testSampleFileGeneratedOnWindows.txt").getPath()),
				dataDescriptionWindowsTestFile);
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

	@Test
	public void testReadFromBigFile() throws IOException {
		int numberOfLines = 0;
		while (true) {
			MutableCharArrayString[] data = fileReaderWithBlankLineAtEOF.read();
			if (data == null) {
				break;
			}
			Assert.assertNotEquals(0, data.length);
			Assert.assertNotEquals(0, data[0].length());
			numberOfLines++;
		}
		Assert.assertEquals(999993, numberOfLines);
	}

	@Test
	public void testReadFileCreatedOnWindows() throws IOException {
		int numberOfLines = 0;
		while (true) {
			MutableCharArrayString[] data = testSampleFileGeneratedOnWindows.read();
			if (data == null) {
				break;
			}
			Assert.assertEquals(1, data[0].length());
			Assert.assertEquals(2, data[1].length());
			numberOfLines++;
		}
		Assert.assertEquals(5, numberOfLines);
	}

	@After
	public void tearDown() throws IOException {
		fileReader.close();
		fileReaderBlankLinesFile.close();
		fileReaderBlankLineAtEofFile.close();
		fileReaderWithBlankLineAtEOF.close();
		testSampleFileGeneratedOnWindows.close();
	}

}
