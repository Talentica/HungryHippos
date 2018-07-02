/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryHippos.master;

import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 27/10/17.
 */
public class UploadedFileStatusChecker implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UploadedFileStatusChecker.class);

    private Queue<UploadedFileStatusStreamDetail> uploadedFileStatusStreamDetails;

    private volatile static int streamSeq = 0;

    private static Object lock = new Object();

    public UploadedFileStatusChecker(Queue<UploadedFileStatusStreamDetail> uploadedFileStatusStreamDetails) {
        this.uploadedFileStatusStreamDetails = uploadedFileStatusStreamDetails;
    }

    @Override
    public Boolean call() throws Exception {
        if (uploadedFileStatusStreamDetails.isEmpty()) {
            return true;
        }
        boolean success = true;
        while (!uploadedFileStatusStreamDetails.isEmpty()) {
            UploadedFileStatusStreamDetail uploadedFileStatusStreamDetail;

            synchronized (lock){
                uploadedFileStatusStreamDetail = uploadedFileStatusStreamDetails.poll();
            }

            if(uploadedFileStatusStreamDetail!=null){
                LOGGER.info("Waiting for status of chunk id: {} Number of chunks successfully processed: {}", uploadedFileStatusStreamDetail.getChunkId(), streamSeq);
                DataOutputStream dataOutputStream = uploadedFileStatusStreamDetail.getDataOutputStream();
                dataOutputStream.writeBoolean(true);
                dataOutputStream.flush();
                String status = uploadedFileStatusStreamDetail.getDataInputStream().readUTF();
                if (!HungryHippoServicesConstants.SUCCESS.equals(status)) {
                    LOGGER.error("Failed uploaded chunk id: {} Number of chunks successfully processed: {} ip:{}",
                            uploadedFileStatusStreamDetail.getChunkId(), streamSeq,uploadedFileStatusStreamDetail.getSocket().getInetAddress().getHostAddress());
                    success =  false;
                }else{
                    streamSeq++;
                }
                uploadedFileStatusStreamDetail.getSocket().close();
                LOGGER.info("Successfully uploaded chunk: {} Number of chunks successfully processed: {}", uploadedFileStatusStreamDetail.getChunkId(), streamSeq);
            }

        }
        return success;
    }
}
