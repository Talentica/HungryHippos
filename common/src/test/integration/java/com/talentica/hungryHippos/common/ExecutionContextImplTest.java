package com.talentica.hungryHippos.common;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FileSystemContext.class)
public class ExecutionContextImplTest {

  private String tempDir;

  @Before
  public void setup() {
    tempDir = getClass().getClassLoader().getResource("").getPath() + File.separator + "temp";
  }

  @Test
  public void testConstructor() {
    String directory = tempDir + File.separator
        + System.currentTimeMillis();
    PowerMockito.mockStatic(FileSystemContext.class);
    PowerMockito.when(FileSystemContext.getRootDirectory()).thenReturn(directory);
    new ExecutionContextImpl(null, directory + File.separator + "test.output");
    new ExecutionContextImpl(null, directory + File.separator + "test.output");
  }

  @Test(expected = RuntimeException.class)
  public void testConstructorWhenFileExists() {
    long currentTimeMillis = System.currentTimeMillis();
    String directory = tempDir + File.separator;
    String filepath = directory + File.separator + currentTimeMillis;
    try {
      new File(directory).mkdirs();
      new File(filepath).createNewFile();
      PowerMockito.mockStatic(FileSystemContext.class);
      PowerMockito.when(FileSystemContext.getRootDirectory()).thenReturn(filepath);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    new ExecutionContextImpl(null,
        filepath + File.separator + "test.output");
  }

  @After
  public void teardown() {
    try {
      FileUtils.deleteDirectory(new File(tempDir));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
