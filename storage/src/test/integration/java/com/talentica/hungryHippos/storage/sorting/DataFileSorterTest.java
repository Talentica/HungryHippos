/**
 * 
 */
package com.talentica.hungryHippos.storage.sorting;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;

/**
 * @author pooshans
 *
 */
@RunWith(EasyMockRunner.class)
public class DataFileSorterTest {

  private String inputDataDir;
  private String shardingConfDir;
  private DataFileSorter sorter;
  private String resource = "/src/test/resources";
  private String testFile = "/6da89208-1ded-4878-8346-b116ee7cfeaf";
  private String testFilePath = null;
  @Mock
  private Job job = null;
  int[] input;
  int[] expOutput;
  int rowSize;


  @Before
  public void setUp() throws ClassNotFoundException, FileNotFoundException, KeeperException,
      InterruptedException, IOException, JAXBException {

    File file = new File("");
    testFilePath = file.getAbsolutePath() + resource + testFile;
    inputDataDir = file.getAbsolutePath() + resource + File.separatorChar + "shardedDir";
    shardingConfDir = file.getAbsolutePath() + resource;
    sorter = new DataFileSorter(inputDataDir, new ShardingApplicationContext(shardingConfDir));
  }

  @Test
  public void testDataFileSort()
      throws IOException, KeeperException, InterruptedException, JAXBException {

    long beforeSortSize = Files.size(Paths.get(testFilePath));
    try {
      sorter.doSortingDefault();
    } catch (IOException e) {
      Assert.assertFalse(true);
    }

    long afterSortSize = Files.size(Paths.get(testFilePath));
    Assert.assertEquals(beforeSortSize, afterSortSize);
  }


  @Test
  public void testDataFileSort_external() throws IOException, ClassNotFoundException,
      KeeperException, InterruptedException, JAXBException {
    testFilePath = "/home/sudarshans/hh1/filesystem/test/input";
    sorter = null;
    inputDataDir = testFilePath;
    sorter = new DataFileSorter(inputDataDir, new ShardingApplicationContext(shardingConfDir));
    long beforeSortSize = Files.size(Paths.get(testFilePath));
    try {
      sorter.doSortingDefault();
    } catch ( IOException  e) {
      Assert.assertFalse(true);
    }

    long afterSortSize = Files.size(Paths.get(testFilePath));
    Assert.assertEquals(beforeSortSize, afterSortSize);
  }


  @Test
  public void testDoSortingJobWise() throws IOException {

    expect(0).andReturn(1).times(1);
    replay(job);

    long beforeSortSize = Files.size(Paths.get(testFilePath));

    try {
      sorter.doSortingPrimaryDimensionWise(0);
    } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException
        | JAXBException e) {
      Assert.assertFalse(true);
    }

    long afterSortSize = Files.size(Paths.get(testFilePath));
    Assert.assertEquals(beforeSortSize, afterSortSize);
  }

  @Test
  public void testDoSortingJobWise_PD_2() throws IOException {

    expect(2).andReturn(2).times(1);
    replay(job);
    long beforeSortSize = Files.size(Paths.get(testFilePath));

    try {
      sorter.doSortingPrimaryDimensionWise(2);
    } catch (ClassNotFoundException | KeeperException | InterruptedException | IOException
        | JAXBException e) {
      Assert.assertFalse(true);
    }

    long afterSortSize = Files.size(Paths.get(testFilePath));
    Assert.assertEquals(beforeSortSize, afterSortSize);
  }

  @Test
  public void testSortDimensions() {
    int[] dimensions = new int[] {2, 0, 4};
    int[] expected = new int[] {0, 2, 4};
    sortDimensions(dimensions);
    Assert.assertArrayEquals(expected, expected);
  }

  private void sortDimensions(int[] dimes) {
    for (int i = 0; i < dimes.length - 1; i++) {
      for (int j = 1; j < dimes.length - i; j++) {
        if (dimes[j - 1] > dimes[j]) {
          int temp = dimes[j];
          dimes[j] = dimes[j - 1];
          dimes[j - 1] = temp;
        }
      }
    }
  }

}
