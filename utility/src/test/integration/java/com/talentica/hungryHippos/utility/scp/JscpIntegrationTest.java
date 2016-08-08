package com.talentica.hungryHippos.utility.scp;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.jcraft.jsch.JSchException;

public class JscpIntegrationTest {

  private static final String SOURCE_FOLDER = "/home/nitink/hungryhippos";

  private static final String DESTINATION_FOLDER = "/root";

  private SecureContext context;

  @Before
  public void setup() {
    context = new SecureContext("root", "138.68.27.207");
    context.setTrustAllHosts(true);
    context.setPrivateKeyFile(
        new File(getClass().getClassLoader().getResource("new_scp_test_key").getFile()));
  }

  @Test
  public void testExec() throws IOException, JSchException {
    Jscp.scpTarGzippedFile(context, SOURCE_FOLDER, DESTINATION_FOLDER, "test-copy");
  }

}
