package com.talentica.hungryhippos.filesystem.util;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ProcessServiceImplTest {

  private Process ExistingProcess = null;
  private Process NonExistingProcess = null;
  private Process killProcess = null;
  private ProcessServiceImpl ps = null;

  @Before
  public void setUp() throws Exception {
    ExistingProcess = new Process("Zookeeper", "29555", "sudarshans", "localhost", "10:24");
    NonExistingProcess = new Process("Zookeeper", "2955", "sudarshans", "localhost", "10:24");
    killProcess = new Process("Zookeeper", "29347", "sudarshans", "localhost", "10:24");
    ps = new ProcessServiceImpl();
  }

  @After
  public void tearDown() throws Exception {
    ExistingProcess = null;
    killProcess = null;
    NonExistingProcess = null;
    ps = null;
  }

  @Test
  public void testCheckProcessIsAlive() {

    ps.checkProcessIsAlive(ExistingProcess);
    assertTrue(ExistingProcess.isAlive());
  }

  @Test
  public void testCheckProcessIsAliveFail() {   
    ps.checkProcessIsAlive(NonExistingProcess);
    assertFalse(NonExistingProcess.isAlive());
  }

  @Test
  public void testKillProcessNotExisting() {
    ps.killProcess(NonExistingProcess);
    assertFalse(NonExistingProcess.isAlive());
  }

  @Test
  public void testKillProcess() {
    ps.killProcess(killProcess);
    assertFalse(killProcess.isAlive());
  }
}
