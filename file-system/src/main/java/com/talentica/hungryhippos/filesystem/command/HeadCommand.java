package com.talentica.hungryhippos.filesystem.command;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import javax.xml.bind.JAXBException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.sharding.Column;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
import com.talentica.hungryhippos.filesystem.client.DataRetrieverClient;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.helper.ShardedFile;

/**
 * {@code HeadCommand} used for printing input or output file of the system to hhfs console. options
 * possible are "-n" => number of lines to read. "-s" => the sharded file that is associated with
 * it. "-r" => node ip from where the client want to read data from. "-o" => location of the sharded
 * file. "-h" => help.
 * 
 * @author sudarshans
 *
 */
public class HeadCommand {

  private static Options options = new Options();
  static {
    options.addOption("n", "number", true, "number of lines to be read");
    options.addOption("s", "sharded", true, "if the file that has to be read is sharded");
    options.addOption("r", "remote location", true, "the remote ip from where you want to read");
    options.addOption("o", "location of sharded file", true, "the location of the sharded file");
    options.addOption("h", "help", false, "");
  }

  /**
   * used for executing head command on the basis of arguments passed.
   * 
   * @param parser
   * @param args
   */
  public static void execute(CommandLineParser parser, String... args) {
    try {
      CommandLine line = parser.parse(options, args);
      int numberOfLines = 10;
      if (line.hasOption("h")) {
        usage();
        return;
      }
      String fileName = null;
      String fileToRead = null;
      if (line.getArgList().size() > 1) {
        String filePath = line.getArgList().get(0);
        String[] dir = filePath.split(FileSystemConstants.ZK_PATH_SEPARATOR);
        fileName = dir[dir.length - 1];
        fileToRead = System.getProperty("user.home") + File.separatorChar + fileName
            + File.separatorChar + line.getArgList().get(2);

      }
      String shardingClientConfigLoc =
          System.getProperty("user.home") + File.separatorChar + fileName + File.separatorChar
              + "sharding-table" + File.separatorChar + "sharding-client-config.xml";

      try {
        if (line.hasOption("n")) {
          numberOfLines = Integer.valueOf(line.getOptionValue("n"));
        }

        if (line.hasOption("s") && line.hasOption("r")) {
          String nodeIp = line.getOptionValue("r");

          FieldTypeArrayDataDescription dataDescription = read(shardingClientConfigLoc);
          int sizeOfLine = dataDescription.getSize();
          int bufferSize = sizeOfLine * 20;

          int port = FileSystemContext.getServerPort();
          int noOfAttempts = FileSystemContext.getMaxQueryAttempts();

          long retryTimeInterval = FileSystemContext.getQueryRetryInterval();

          DataRetrieverClient.retrieveDataBlocks_test(nodeIp, fileToRead, bufferSize, sizeOfLine,
              port, retryTimeInterval, noOfAttempts, shardingClientConfigLoc);
        } else if (line.hasOption("s") && line.hasOption("o")) {
          fileToRead = line.getOptionValue("o");
          String config = line.getOptionValue("s");
          if (config != null) {
            shardingClientConfigLoc = config;
          }
          ShardedFile.read(fileToRead, shardingClientConfigLoc, numberOfLines);
        } else if (line.hasOption("s")) {
          ShardedFile.read(fileToRead, shardingClientConfigLoc, numberOfLines);
        } else if (line.hasOption("r")) {
          String nodeIp = line.getOptionValue("r");
          int port = FileSystemContext.getServerPort();
          int noOfAttempts = FileSystemContext.getMaxQueryAttempts();
          int bufferSize = FileSystemContext.getFileStreamBufferSize();

          long retryTimeInterval = FileSystemContext.getQueryRetryInterval();

          DataRetrieverClient.retrieveDataBlocks_output(nodeIp, fileToRead, bufferSize, port,
              retryTimeInterval, noOfAttempts);
        } else {
          try (Stream<String> stream = Files.lines(Paths.get(fileName))) {

            stream.limit(numberOfLines).forEach(System.out::println);

          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

    } catch (ParseException e) {

      e.printStackTrace();
    }

  }

  public static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("head", options);
  }

  private static FieldTypeArrayDataDescription read(String shardingClientConfigLoc)
      throws IOException, JAXBException {
    ShardingClientConfig shardedConfig =
        JaxbUtil.unmarshalFromFile(shardingClientConfigLoc, ShardingClientConfig.class);
    List<Column> columns = shardedConfig.getInput().getDataDescription().getColumn();
    String[] dataTypeDescription = new String[columns.size()];
    FieldTypeArrayDataDescription dataDescription = null;
    for (int index = 0; index < columns.size(); index++) {
      String element = columns.get(index).getDataType() + "-" + columns.get(index).getSize();
      dataTypeDescription[index] = element;
    }
    dataDescription = FieldTypeArrayDataDescription.createDataDescription(dataTypeDescription,
        shardedConfig.getMaximumSizeOfSingleBlockData());
    return dataDescription;
  }


}
