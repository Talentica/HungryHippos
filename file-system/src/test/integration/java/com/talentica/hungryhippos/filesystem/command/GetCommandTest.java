package com.talentica.hungryhippos.filesystem.command;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryhippos.filesystem.Exception.HungryHipposFileSystemException;
import com.talentica.hungryhippos.filesystem.main.HungryHipposFileSystemMain;


@Ignore
public class GetCommandTest {

  String folder = "/sudarshans/input";
  String command = "get";

  String clientConfig =
      "//home//sudarshans//RD//HH_NEW//HungryHippos//configuration-schema//src//main//resources//distribution//client-config.xml";


  @Before
  public void setUp() throws HungryHipposFileSystemException {
 

  }

  @Test
  public void testExecute_get() {
    String option_s = "-s";
    CommandLineParser parser = new DefaultParser();
    String[] args = {command, folder, option_s};
    GetCommand.execute(parser, args);
  }

}
