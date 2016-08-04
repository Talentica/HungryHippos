package com.talentica.hungryHippos.job.util;

import com.talentica.hungryHippos.coordination.utility.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Created by rajkishoreh on 2/8/16.
 */
public class JobIDGenerator {

    private static final Logger logger = LoggerFactory.getLogger(JobIDGenerator.class);

    /**
     * Generates a random ID for a job
     * @return
     */
    public static String generateJobID(){
        String uuid = UUID.randomUUID().toString();
        String jobUUIdInBase64 = CommonUtil.getJobUUIdInBase64(uuid);
        return jobUUIdInBase64;
    }

}
