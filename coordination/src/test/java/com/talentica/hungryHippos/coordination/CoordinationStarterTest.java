package com.talentica.hungryHippos.coordination;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import javax.xml.bind.JAXBException;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.sun.org.apache.bcel.internal.util.ClassLoader;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.context.DataPublisherApplicationContext;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.coordination.CoordinationConfig;
import com.talentica.hungryhippos.config.coordination.ZookeeperDefaultConfig;
import com.talentica.hungryhippos.config.datapublisher.DatapublisherConfig;

import junit.framework.Assert;

import static junit.framework.Assert.assertTrue;

@Ignore
public class CoordinationStarterTest {

  private static final String USER_HOME = System.getProperty("user.home");
  private static final String ZOOKEEPER = "zookeeper";
  private static String ZOOKEEPER_PATH = null;
  private String[] args = new String[6];
  private String clientConfigXml = "config/client-config.xml";
  private String coordinationConfigXml = "config/coordination-config.xml";
  private String clusterConfigXml = "config/cluster-config.xml";
  private String datapublisherConfigXml = "config/datapublisher-config.xml";
  private String filesystemConfigXml = "config/filesystem-config.xml";
  private String jobRunnerConfigXml = "config/job-runner-config.xml";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    // check whether zookeeper process is running
    boolean isZRun = runPs();

    // if zookeeper process is not running
    if (!isZRun) {
      // check whether zookeeper is installed in the system.
      fileExits(new File(USER_HOME));

      // wasn't able to find zookeeper installation .
      if (ZOOKEEPER_PATH == null) {
        // download zookeeper to user.home dir.
        downloadZookeeper();

        File file =
            unGzip(new File(USER_HOME + File.separatorChar + "zookeeper-3.5.1-alpha.tar.gz"),
                new File(USER_HOME));
        unTar(file, new File(USER_HOME));
        file.delete();

        ZOOKEEPER_PATH = USER_HOME + File.separatorChar + "zookeeper-3.5.1-alpha";

        Runtime.getRuntime().exec("chmod -R 777 " + ZOOKEEPER_PATH);
        Runtime.getRuntime().exec("cp " + USER_HOME + "/zookeeper-3.5.1-alpha/conf/zoo_sample.cfg "
            + USER_HOME + "/zookeeper-3.5.1-alpha/conf/zoo.cfg");


        Thread.sleep(4000);
      }

      // run zookeeper script for starting zookeeper which is zkStart.sh start
      runScripts();

    }



  }

  /**
   * Untar an input file into an output file.
   * 
   * The output file is created in the output folder, having the same name as the input file, minus
   * the '.tar' extension.
   * 
   * @param inputFile the input .tar file
   * @param outputDir the output directory file.
   * @throws IOException
   * @throws FileNotFoundException
   * 
   * @return The {@link List} of {@link File}s with the untared content.
   * @throws ArchiveException
   */
  private static List<File> unTar(final File inputFile, final File outputDir)
      throws FileNotFoundException, IOException, ArchiveException {

    final List<File> untaredFiles = new LinkedList<File>();
    final InputStream is = new FileInputStream(inputFile);
    final TarArchiveInputStream debInputStream =
        (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is);
    TarArchiveEntry entry = null;
    while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
      final File outputFile = new File(outputDir, entry.getName());
      if (entry.isDirectory()) {

        if (!outputFile.exists()) {

          if (!outputFile.mkdirs()) {
            throw new IllegalStateException(
                String.format("Couldn't create directory %s.", outputFile.getAbsolutePath()));
          }
        }
      } else {

        final OutputStream outputFileStream = new FileOutputStream(outputFile);
        IOUtils.copy(debInputStream, outputFileStream);
        outputFileStream.close();
      }
      untaredFiles.add(outputFile);
    }
    debInputStream.close();

    return untaredFiles;
  }

  /**
   * Ungzip an input file into an output file.
   * <p>
   * The output file is created in the output folder, having the same name as the input file, minus
   * the '.gz' extension.
   * 
   * @param inputFile the input .gz file
   * @param outputDir the output directory file.
   * @throws IOException
   * @throws FileNotFoundException
   * 
   * @return The {@File} with the ungzipped content.
   */
  private static File unGzip(final File inputFile, final File outputDir)
      throws FileNotFoundException, IOException {



    final File outputFile =
        new File(outputDir, inputFile.getName().substring(0, inputFile.getName().length() - 3));

    final GZIPInputStream in = new GZIPInputStream(new FileInputStream(inputFile));
    final FileOutputStream out = new FileOutputStream(outputFile);

    IOUtils.copy(in, out);

    in.close();
    out.close();

    return outputFile;
  }



  private static boolean downloadZookeeper() throws IOException {

    ProcessBuilder builder = new ProcessBuilder("/bin/sh", "-c", "wget -O  " + USER_HOME
        + File.separatorChar + "zookeeper-3.5.1-alpha.tar.gz "
        + " http://www-us.apache.org/dist/zookeeper/zookeeper-3.5.1-alpha/zookeeper-3.5.1-alpha.tar.gz ");

    return execute(builder);
  }


  private static boolean runPs() throws IOException {
    ProcessBuilder builder = new ProcessBuilder("/bin/sh", "-c", "ps -ef | grep QuorumPeerMain");

    return execute(builder);

  }

  private static boolean execute(ProcessBuilder builder) {
    Process process;
    boolean flag = false;
    try {
      process = builder.start();

      int processStatus = process.waitFor();
      BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      String line = null;
      StringBuilder sb = new StringBuilder();
      while ((line = br.readLine()) != null) {
        sb.append(line).append("\n");
      }
      System.out.println(sb.toString());

      br = new BufferedReader(new InputStreamReader(process.getInputStream()));

      while ((line = br.readLine()) != null) {

        if (line.contains("java -Dzookeeper.log.dir")) {
          flag = true;
        }

      }

      if (processStatus != 0) {
        throw new RuntimeException("Operation " + builder.command() + " failed");
      }
    } catch (IOException | InterruptedException e1) {
      e1.printStackTrace();
      throw new RuntimeException(e1);
    }

    return flag;
  }

  private static void runScripts() throws IOException {
    ProcessBuilder builder =
        new ProcessBuilder("/bin/bash", "-c", ZOOKEEPER_PATH + "/bin/zkServer.sh start");
    execute(builder);
  }

  private static void fileExits(File currentFile) throws IOException {


    if (ZOOKEEPER_PATH != null) {
      return;
    }



    File[] files = currentFile.listFiles(new FilenameFilter() {


      @Override
      public boolean accept(File dir, String name) {
        if (name.startsWith(".")) {
          return false;
        }
        return true;
      }
    });



    if (files.length == 0) {
      return;
    } else {

      for (File file : files) {

        if (file.isFile()) {

          if (file.getAbsolutePath().endsWith("bin/zkServer.sh")) {
            ZOOKEEPER_PATH = file.toString();
            return;
          }
        } else {
          fileExits(file);
        }
      }

    }

    return;


  }


  @AfterClass
  public static void tearDownAfterClass() throws Exception {

    // Runtime.getRuntime().exec("pkill -f \"QuorumPeerMain\"");
  }

  @Before
  public void setUp() throws Exception {
    args[0] = ClassLoader.getSystemClassLoader().getResource(clientConfigXml).getPath();
    args[1] = ClassLoader.getSystemClassLoader().getResource(coordinationConfigXml).getPath();
    args[2] = ClassLoader.getSystemClassLoader().getResource(clusterConfigXml).getPath();
    args[3] = ClassLoader.getSystemClassLoader().getResource(datapublisherConfigXml).getPath();
    args[4] = ClassLoader.getSystemClassLoader().getResource(filesystemConfigXml).getPath();
    args[5] = ClassLoader.getSystemClassLoader().getResource(jobRunnerConfigXml).getPath();

  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testMain() throws FileNotFoundException, JAXBException, HungryHippoException {

    CoordinationStarter.main(args);

    // start verifying

    HungryHippoCurator hungryHippoCurator = HungryHippoCurator.getInstance("localhost:2181");

    // verify coordinationConfig details -> args[1]
    CoordinationConfig coordinationConfig =
        JaxbUtil.unmarshalFromFile(args[1], CoordinationConfig.class);

    ZookeeperDefaultConfig zkDefaultConfig = coordinationConfig.getZookeeperDefaultConfig();

    assertTrue(hungryHippoCurator.checkExists(zkDefaultConfig.getAlertPath()));
    assertTrue(hungryHippoCurator.checkExists(zkDefaultConfig.getCleanup()));
    assertTrue(hungryHippoCurator.checkExists(zkDefaultConfig.getFileidHhfsMapPath()));
    assertTrue(hungryHippoCurator.checkExists(zkDefaultConfig.getFilesystemPath()));
    assertTrue(hungryHippoCurator.checkExists(zkDefaultConfig.getHostPath()));
    assertTrue(hungryHippoCurator.checkExists(zkDefaultConfig.getJobConfigPath()));
    assertTrue(hungryHippoCurator.checkExists(zkDefaultConfig.getJobStatusPath()));
    assertTrue(hungryHippoCurator.checkExists(zkDefaultConfig.getNamespacePath()));
    assertTrue(hungryHippoCurator.checkExists(zkDefaultConfig.getRetry()));
    assertTrue(hungryHippoCurator.checkExists(zkDefaultConfig.getShardingTablePath()));
    assertTrue(hungryHippoCurator.checkExists(zkDefaultConfig.getTickTime()));


    // verify clusterConfig details -> args[2]
    ClusterConfig clusterConfig = JaxbUtil.unmarshalFromFile(args[2], ClusterConfig.class);

    ClusterConfig clusterConfigInZK = (ClusterConfig) hungryHippoCurator
        .readObject(CoordinationConfigUtil.getProperty().getValueByKey("zookeeper.config_path")
            + File.separatorChar + CoordinationConfigUtil.CLUSTER_CONFIGURATION);

    Assert.assertEquals(clusterConfig.getNode().size(), clusterConfigInZK.getNode().size());
    assertTrue(clusterConfig.getNode().equals(clusterConfigInZK.getNode()));

    // verify datapublisher.xml -> args[3]

    DatapublisherConfig dataPublisherConfig =
        JaxbUtil.unmarshalFromFile(args[3], DatapublisherConfig.class);
    DatapublisherConfig dataPublisherConfigInZK = (DatapublisherConfig) hungryHippoCurator
        .readObject(CoordinationConfigUtil.getProperty().getValueByKey("zookeeper.config_path")
            + File.separatorChar + CoordinationConfigUtil.DATA_PUBLISHER_CONFIGURATION);

    Assert.assertEquals(dataPublisherConfig.getNoOfAttemptsToConnectToNode(),
        dataPublisherConfigInZK.getNoOfAttemptsToConnectToNode());
    Assert.assertEquals(dataPublisherConfig.getNoOfBytesInEachMemoryArray(),
        dataPublisherConfigInZK.getNoOfBytesInEachMemoryArray());
    Assert.assertEquals(dataPublisherConfig.getNoOfDataReceiverThreads(),
        dataPublisherConfigInZK.getNoOfDataReceiverThreads());
    Assert.assertEquals(dataPublisherConfig.getServersConnectRetryIntervalInMs(),
        dataPublisherConfigInZK.getServersConnectRetryIntervalInMs());


    // verify filesystem-config.xml -> args[3]

  }



}