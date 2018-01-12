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

package com.talentica.hungryHippos.node.datareceiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by rajkishoreh on 12/7/17.
 */
public class IncrementalDataStatusChecker extends Thread{
    private static final Logger logger = LoggerFactory.getLogger(IncrementalDataStatusChecker.class);
    private Queue<Future<Boolean>> futures;
    private boolean success;
    private volatile boolean keepAlive;

    public IncrementalDataStatusChecker(Queue<Future<Boolean>> futures) {
        this.futures = futures;
        keepAlive = true;
        success =true;
    }

    @Override
    public void run() {
        while(keepAlive){
            Future<Boolean> future = futures.poll();
            if(future!=null){
                while(future!=null){
                    try {
                        if(!future.isDone()){
                            while(!futures.offer(future));
                        }else if(!future.get()){
                            success =false;
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        success =false;
                        e.printStackTrace();
                        logger.error(e.getMessage());
                    }
                    future = futures.poll();
                }
                System.gc();
            }else{
                System.gc();
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }


    public boolean isSuccess() {
        return success;
    }

    public void kill(){
        this.keepAlive = false;
    }

}
