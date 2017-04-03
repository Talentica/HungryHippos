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
package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rajkishoreh on 12/1/17.
 */
public class MetaDataUploader implements Runnable {


    private static final Logger logger = LoggerFactory.getLogger(MetaDataUploader.class);

    private Node node ;
    private String metadatFilerPath;
    private boolean success;
    private CountDownLatch countDownLatch;
    private String hhFilePath;


    public MetaDataUploader(CountDownLatch countDownLatch, Node node, String metadatFilerPath , String hhFilePath) {
        this.node = node;
        this.metadatFilerPath = metadatFilerPath;
        this.success = false;
        this.countDownLatch = countDownLatch;
        this.hhFilePath = hhFilePath;
    }

    @Override
    public void run() {
        Socket socket = null;
        try {
            socket = ServerUtils.connectToServer(node.getIp() + ":" + node.getPort(), 50);
            File metadataFile = new File(metadatFilerPath);
            long metadatFileSize = metadataFile.length();
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            dos.writeInt(HungryHippoServicesConstants.METADATA_UPDATER);
            dos.writeUTF(hhFilePath);
            dos.writeUTF(metadatFilerPath);
            dos.writeLong(metadatFileSize);
            int bufferSize = 2048;
            byte[] buffer = new byte[bufferSize];
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(metadataFile),bufferSize*10);
            int len;
            while((len=bis.read(buffer))>-1){
                dos.write(buffer,0,len);
            }
            dos.flush();
            bis.close();
            String response = dis.readUTF();
            if(!HungryHippoServicesConstants.SUCCESS.equals(response)){
                success = false;
            }else{
                success = true;
            }
        } catch (IOException |InterruptedException e) {
            logger.error("Failed to upload metadata to {} for {} Reason: ",node.getIp(),metadatFilerPath,e.getMessage() );
            e.printStackTrace();
        }finally {
            countDownLatch.countDown();
            if(socket!=null){
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public Node getNode() {
        return node;
    }

    public boolean isSuccess() {
        return success;
    }
}
