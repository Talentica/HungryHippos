package com.talentica.hungryHippos.utility.marshaling;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.data.parser.CsvDataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileReader;
import com.talentica.hungryHippos.coordination.utility.marshaling.FileWriter;

public class FileReaderTest {

  private FileReader fileReader;

  private FileReader fileReaderBlankLinesFile;

  private FileReader fileReaderBlankLineAtEofFile;

  private FileReader fileReaderWithBlankLineAtEOF;

  private FileReader testSampleFileGeneratedOnWindows;

  private String badRecordsFile;

  @Before
  public void setUp() throws IOException {
    ClassLoader classLoader = this.getClass().getClassLoader();
    DataDescription dataDescription = FieldTypeArrayDataDescription.createDataDescription(
        "STRING-1,STRING-1,STRING-1,STRING-1,DOUBLE-0,DOUBLE-0,DOUBLE-0,DOUBLE-0,STRING-3"
            .split(","),
        100);
    CsvDataParser csvDataPreprocessor = new CsvDataParser(dataDescription);
    fileReader = new FileReader(
        new File(classLoader.getResource("testSampleInputWithNoBlankLineAtEOF.txt").getPath()),
        csvDataPreprocessor);
    fileReaderBlankLinesFile = new FileReader(
        new File(classLoader.getResource("testSampleInputWithBlankLines.txt").getPath()),
        csvDataPreprocessor);
    fileReaderBlankLineAtEofFile = new FileReader(
        new File(classLoader.getResource("testSampleInputWithBlankLines.txt").getPath()),
        csvDataPreprocessor);
    fileReaderWithBlankLineAtEOF = new FileReader(
        new File(classLoader.getResource("testSampleInputWithBlankLineAtEOF.txt").getPath()),
        csvDataPreprocessor);
    DataDescription dataDescriptionWindowsTestFile =
        FieldTypeArrayDataDescription.createDataDescription("STRING-3,LONG-0".split(","), 100);
    CsvDataParser csvDataPreprocessorForWindowsTestFile =
        new CsvDataParser(dataDescriptionWindowsTestFile);
    testSampleFileGeneratedOnWindows = new FileReader(
        new File(classLoader.getResource("testSampleFileGeneratedOnWindows.txt").getPath()),
        csvDataPreprocessorForWindowsTestFile);
    badRecordsFile = new File("").getAbsolutePath() + File.separator + "test.err";
  }


  @Test
  public void testRead() throws IOException, InvalidRowException {
    int numberOfLines = 0;
    while (true) {
      DataTypes[] data = fileReader.read();
      if (data == null) {
        break;
      }
      Assert.assertNotEquals(0, data.length);
      Assert.assertNotEquals(0, data[0].getLength());
      numberOfLines++;
    }
    Assert.assertEquals(2, numberOfLines);
  }


  @Test
  public void testReadWithNoBlankLineAtTheEndOfFile() throws IOException, InvalidRowException {
    int numberOfLines = 0;
    while (true) {
      DataTypes[] data = fileReaderBlankLineAtEofFile.read();
      if (data == null) {
        break;
      }
      Assert.assertNotEquals(0, data.length);
      Assert.assertNotEquals(0, data[0].getLength());
      numberOfLines++;
    }
    Assert.assertEquals(4, numberOfLines);
  }


  @Test
  public void testReadFromFileHavingBlankLines() throws IOException, InvalidRowException {
    int numberOfLines = 0;
    while (true) {
      DataTypes[] data = fileReaderBlankLinesFile.read();
      if (data == null) {
        break;
      }
      Assert.assertNotEquals(0, data.length);
      Assert.assertNotEquals(0, data[0].getLength());
      numberOfLines++;
    }
    Assert.assertEquals(4, numberOfLines);
  }

  @Test
  public void testReadFromBigFile() throws IOException, InvalidRowException {
    int numberOfLines = 0;
    FileWriter fileWriter = new FileWriter(badRecordsFile);
    fileWriter.openFile();
    while (true) {

      DataTypes[] data = null;
      try {
        data = fileReaderWithBlankLineAtEOF.read();
      } catch (InvalidRowException e) {
        fileWriter.flushData(numberOfLines++, e);
        continue;
      }

      if (data == null) {
        break;
      }
      Assert.assertNotEquals(0, data.length);
      Assert.assertNotEquals(0, data[0].getLength());
      numberOfLines++;
    }
    fileWriter.close();
    Assert.assertEquals(999993, numberOfLines);
  }

  @Test
  public void testReadFileCreatedOnWindows() throws IOException, InvalidRowException {
    int numberOfLines = 0;
    while (true) {
      DataTypes[] data = testSampleFileGeneratedOnWindows.read();
      if (data == null) {
        break;
      }
      Assert.assertEquals(1, data[0].getLength());
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
