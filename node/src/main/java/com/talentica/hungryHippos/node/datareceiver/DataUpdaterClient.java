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

import com.talentica.hungryHippos.coordination.server.ServerUtils;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 12/7/17.
 */
public class DataUpdaterClient implements Callable<Boolean> {

    private Node node;
    private Queue<String> dataFileQueue;
    private String hhFilePath;


    public DataUpdaterClient(String hhFilePath,Node node, Queue<String> dataFileQueue) {
        this.hhFilePath = hhFilePath;
        this.node = node;
        this.dataFileQueue = dataFileQueue;
    }

    @Override
    public Boolean call() throws Exception {
        String dataFilePath = dataFileQueue.poll();
        if (dataFilePath == null) return true;
        Socket socket = null;
        try {
            socket = ServerUtils.connectToServer(node.getIp() + ":" + node.getPort(), 50);
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            dos.writeInt(HungryHippoServicesConstants.DATA_UPDATER);
            dos.writeUTF(hhFilePath);
            while (dataFilePath != null) {
                dos.writeBoolean(true);
                File dataFile = new File(dataFilePath);
                long dataFileSize = dataFile.length();
                dos.writeUTF(dataFilePath);
                dos.writeLong(dataFileSize);
                int bufferSize = 2048;
                byte[] buffer = new byte[bufferSize];
                try (FileInputStream fis = new FileInputStream(dataFile);
                     BufferedInputStream bis = new BufferedInputStream(fis)) {
                    int len;
                    while ((len = bis.read(buffer)) > -1) {
                        dos.write(buffer, 0, len);
                    }
                    dos.flush();
                }
                dis.readBoolean();
                dataFilePath = dataFileQueue.poll();
            }
            dos.writeBoolean(false);
            dos.flush();
            String response = dis.readUTF();
            if (!HungryHippoServicesConstants.SUCCESS.equals(response)) {
                throw new RuntimeException("Data Update failure for " + dataFilePath + " ip:" + node.getIp());
            }
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

}
