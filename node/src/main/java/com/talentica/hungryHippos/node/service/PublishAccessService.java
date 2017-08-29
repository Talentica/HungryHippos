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

import com.talentica.hungryHippos.node.DataDistributorStarter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Created by rajkishoreh on 21/12/16.
 */
public class PublishAccessService implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(PublishAccessService.class);
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;

    public PublishAccessService(Socket socket) throws IOException {
        this.socket = socket;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
    }


    @Override
    public void run() {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        logger.info("Publish access request from {}", this.socket.getInetAddress());
        boolean countDecremented = false;
        try {
            int count = DataDistributorStarter.noOfAvailableDataDistributors.decrementAndGet();
            countDecremented = true;
            if(count<0){
                DataDistributorStarter.noOfAvailableDataDistributors.incrementAndGet();
                countDecremented = false;
                this.dataOutputStream.writeBoolean(false);
                this.dataOutputStream.flush();
                logger.info("Slot unavailable: Denied Publish access to {}", this.socket.getInetAddress());
                this.socket.close();
            }else{
                this.dataOutputStream.writeBoolean(true);
                this.dataOutputStream.flush();
                DataDistributorStarter.dataDistributorService.execute(new DataDistributorService(socket));
                countDecremented = false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            if(countDecremented){
                DataDistributorStarter.noOfAvailableDataDistributors.incrementAndGet();
            }
            try {
                this.socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }
}
