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

import java.util.Queue;
import java.util.concurrent.*;

/**
 * Created by rajkishoreh on 27/10/17.
 */
public enum UploadedFileStatusCheckerHandler {
    INSTANCE;
    private Queue<UploadedFileStatusStreamDetail> uploadedFileStatusStreamDetails;
    private Queue<Future<Boolean>> statuses;
    private boolean success;
    private ExecutorService executorService;
    UploadedFileStatusCheckerHandler(){
        success = true;
        uploadedFileStatusStreamDetails = new ConcurrentLinkedQueue<>();
        statuses = new ConcurrentLinkedQueue<>();
        executorService = Executors.newWorkStealingPool();
    }

    public synchronized void receiveStatus(UploadedFileStatusStreamDetail uploadedFileStatusStreamDetail){
        uploadedFileStatusStreamDetails.offer(uploadedFileStatusStreamDetail);
        statuses.offer(executorService.submit(new UploadedFileStatusChecker(uploadedFileStatusStreamDetails)));
    }

    public boolean isSuccess() {
        while(!statuses.isEmpty()){
            Future<Boolean> booleanFuture = statuses.poll();
            try {
                success=success&&booleanFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        executorService.shutdown();
        return success;
    }
}
