/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
    private static final String CLEAR_UNIX_CACHE = "free_up_mem.sh";
    private static AtomicInteger clearSignal = new AtomicInteger(1);

    @Override
    public void run() {
        int clearSignalVal = clearSignal.decrementAndGet();
        if(clearSignalVal==0) {
            try {
                logger.info("[{}] Clearing unix cache", Thread.currentThread().getName());
                Process clearCacheProcess = Runtime.getRuntime().exec(System.getProperty("hh.bin.dir") + File.separator + CLEAR_UNIX_CACHE );

                int clearCacheProcessStatus = clearCacheProcess.waitFor();
                if (clearCacheProcessStatus != 0) {
                    printReasonForAbnormalStatus(clearCacheProcess);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }else{
            logger.info("[{}] Clearing unix cache already in progress", Thread.currentThread().getName());
        }
        clearSignal.incrementAndGet();
    }

    private void printReasonForAbnormalStatus(Process clearCacheProcess) throws IOException {
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
        logger.error("[{}] Error while clearing cache :  {}", Thread.currentThread().getName(), sb.toString());
    }
}
