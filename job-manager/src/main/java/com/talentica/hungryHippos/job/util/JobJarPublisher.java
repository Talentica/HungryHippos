package com.talentica.hungryHippos.job.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.common.context.JobRunnerApplicationContext;

/**
 * Created by rajkishoreh on 2/8/16.
 */
public class JobJarPublisher {

    private static final Logger logger = LoggerFactory.getLogger(JobJarPublisher.class);

    /**
     * Sends the Jar file to each HH node
     * @param jobUUID
     * @param localJarPath
     * @return
     */
    public static boolean publishJar(String jobUUID, String localJarPath) {
        //TODO code for publishing jar
      String jarDirectory = JobRunnerApplicationContext.getZkJobRunnerConfig().getJarRootDirectory();
      String jobJarPath = jarDirectory+ File.separatorChar+jobUUID;
      String command = "cp "+localJarPath+ " "+jobJarPath+"";
      try {
       Runtime.getRuntime().exec("mkdir -p "+jobJarPath);
       Process process=  Runtime.getRuntime().exec(command);
        BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        String line = null;
        StringBuilder sb = new StringBuilder();
        while ((line = br.readLine()) != null){
            sb.append(line).append("\n");
        }
        System.out.println("Console OUTPUT : \n"+sb.toString());
        Runtime.getRuntime().exec("chmod -R 777 "+jobJarPath);
      } catch (IOException e) {
        logger.error(e.toString());
        throw new RuntimeException(e);
      }
        return true;
    }

}
