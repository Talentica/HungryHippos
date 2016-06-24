package com.talentica.hungryHippos.sharding.main;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.coordination.context.CoordinationApplicationContext;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import com.talentica.hungryHippos.coordination.utility.ZkSignalListener;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.Sharding;

public class ShardingStarter {

  /**
   * @param args
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(ShardingStarter.class);
  private static String sampleInputFile;

  public static void main(String[] args) {
    try {
      validateArguments(args);
      initialize(args);
      String dataParserClassName = args[1];
      DataParser dataParser =
          (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class)
              .newInstance(CoordinationApplicationContext.getConfiguredDataDescription());
      LOGGER.info("SHARDING STARTED");
      long startTime = System.currentTimeMillis();
      Sharding.doSharding(getInputReaderForSharding(dataParser));
      LOGGER.info("SHARDING DONE!!");

      long endTime = System.currentTimeMillis();
      LOGGER.info("It took {} seconds of time to do sharding.", ((endTime - startTime) / 1000));
    } catch (Exception exception) {
      LOGGER.error("Error occurred while sharding.", exception);
    }
  }

  private static void validateArguments(String[] args) {
    if (args.length < 2) {
      throw new RuntimeException("Missing job uuid and/or data parser class name parameters.");
    }
  }

  /**
   * @param args
   * @throws Exception
   */
  private static void initialize(String[] args) throws Exception {
    String jobUUId = args[0];
    ZkSignalListener.jobuuidInBase64 = CommonUtil.getJobUUIdInBase64(jobUUId);
    CommonUtil.loadDefaultPath(jobUUId);
  }

  private static Reader getInputReaderForSharding(DataParser dataParser) throws IOException {
    sampleInputFile =
        CoordinationApplicationContext.getProperty().getValueByKey("input.file").toString();
    return new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
        sampleInputFile, dataParser);
  }

}
