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
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.config.cluster.Node;

import java.io.*;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 12/7/17.
 */
public class BlockStatisticsDataUpdaterClient implements Callable<Boolean> {

    private Node node;
    private String hhFilePath;
    private String blockStatisticsFolderPath;


    public BlockStatisticsDataUpdaterClient(String hhFilePath, Node node, String blockStatisticsFolderPath) {
        this.hhFilePath = hhFilePath;
        this.node = node;
        this.blockStatisticsFolderPath = blockStatisticsFolderPath;
    }

    @Override
    public Boolean call() throws Exception {
        Socket socket = null;
        File tarFile = new File(blockStatisticsFolderPath + ".tar.gz");
        long tarFileSize = tarFile.length();
        if(tarFileSize==0) return true;
        try {
            socket = ServerUtils.connectToServer(node.getIp() + ":" + node.getPort(), 50);
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            dos.writeInt(HungryHippoServicesConstants.TAR_DATA_UPDATER);
            dos.writeUTF(hhFilePath);
            dos.writeUTF(blockStatisticsFolderPath);
            dos.writeUTF(tarFile.getAbsolutePath());
            dos.writeLong(tarFileSize);
            int bufferSize = 2048;
            byte[] buffer = new byte[bufferSize];
            try (FileInputStream fis = new FileInputStream(tarFile);
                 BufferedInputStream bis = new BufferedInputStream(fis)) {
                int len;
                while ((len = bis.read(buffer)) > -1) {
                    dos.write(buffer, 0, len);
                }
            }
            dos.flush();
            String response = dis.readUTF();
            if (!HungryHippoServicesConstants.SUCCESS.equals(response)) {
                throw new RuntimeException("Data Update failure for " + blockStatisticsFolderPath + " ip:" + node.getIp());
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
