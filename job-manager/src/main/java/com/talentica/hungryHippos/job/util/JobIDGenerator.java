package com.talentica.hungryHippos.job.util;

import java.util.Base64;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code JobIDGenerator} used for random generation JobID.
 * 
 * @author rajkishoreh
 * @since 2/8/16.
 */
public class JobIDGenerator {

  private static final Logger logger = LoggerFactory.getLogger(JobIDGenerator.class);

  /**
   * Generates a random ID for a job
   * 
   * @return
   */
  public static String generateJobID() {
    String uuid = UUID.randomUUID().toString();
    String jobUUIdInBase64 = Base64.getUrlEncoder().encodeToString(uuid.getBytes());
    logger.debug("JobUUID :- {}, jobUUIdInBase64");
    return jobUUIdInBase64;
  }

}
