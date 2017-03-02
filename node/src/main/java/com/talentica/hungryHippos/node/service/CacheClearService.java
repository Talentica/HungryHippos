package com.talentica.hungryHippos.node.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by rajkishoreh on 28/2/17.
 */
public class CacheClearService implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(CacheClearService.class);
    private static final String CLEAR_UNIX_CACHE = "clear_unix_cache";
    private static final long MIN_FREE_RAM = 1024*1024*1024;
    private static AtomicInteger clearSignal = new AtomicInteger(1);

    @Override
    public void run() {
        int clearSignalVal = clearSignal.decrementAndGet();
        if(clearSignalVal==0) {
            try {
                logger.info("[{}] Clearing unix cache", Thread.currentThread().getName());
                Process clearCacheProcess = Runtime.getRuntime().exec(System.getProperty("hh.bin.dir") + File.separator + CLEAR_UNIX_CACHE + " " + MIN_FREE_RAM);
                int clearCacheProcessStatus = clearCacheProcess.waitFor();
                if (clearCacheProcessStatus != 0) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(clearCacheProcess.getErrorStream()));
                    String line = null;
                    StringBuilder sb = new StringBuilder();
                    while ((line = br.readLine()) != null) {
                        sb.append(line);
                    }
                    br.close();
                    br = new BufferedReader(new InputStreamReader(clearCacheProcess.getInputStream()));
                    while ((line = br.readLine()) != null) {
                        sb.append(line);
                    }
                    br.close();
                    logger.error("[{}] Error while clearing cache for {}:  {}", Thread.currentThread().getName(), sb.toString());
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }else{
            logger.info("[{}] Clearing unix cache already in progress", Thread.currentThread().getName());
        }
        clearSignal.incrementAndGet();
    }
}
