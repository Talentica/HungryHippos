package com.talentica.hungryhippos.filesystem.command;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.filesystem.Exception.HungryHipposFileSystemException;
import com.talentica.hungryhippos.filesystem.main.HungryHipposFileSystemMain;

public class LSCommandTest {

  String folder = "/sudarshans12";
  String command = "ls";
  String option = "-l";
  String clientConfig =
      "//home//sudarshans//RD//HH_NEW//HungryHippos//configuration-schema//src//main//resources//distribution//client-config.xml";

  @Before
  public void setUp() throws HungryHipposFileSystemException {
    try {
      NodesManagerContext.getNodesManagerInstance(clientConfig);
      HungryHipposFileSystemMain.getHHFSInstance();
    } catch (FileNotFoundException | JAXBException e) {
      throw new HungryHipposFileSystemException(e.getMessage());
    }
  }

  @Test
  public void testExecute_ls() {
    CommandLineParser parser = new DefaultParser();
    String[] args = {command, folder};
    LSCommand.execute(parser, args);
  }


  @Test
  public void testExecute_ls_l() {
    CommandLineParser parser = new DefaultParser();
    String[] args = {command, folder, option};
    LSCommand.execute(parser, args);
  }
}