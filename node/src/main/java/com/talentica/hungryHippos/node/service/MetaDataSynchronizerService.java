/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.datareceiver.*;
import com.talentica.hungryHippos.node.joiners.FileJoinCaller;
import com.talentica.hungryHippos.node.joiners.FirstStageNodeFileJoinerCaller;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

/**
 * Created by rajkishoreh on 6/2/17.
 */
public class MetaDataSynchronizerService implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(MetaDataSynchronizerService.class);
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;

    public MetaDataSynchronizerService(Socket socket) throws IOException {
        this.socket = socket;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
    }


    @Override
    public void run() {

        String hhFilePath = null;
        try {
            hhFilePath = dataInputStream.readUTF();


            logger.info("Checking publish status of {}",hhFilePath);
            boolean status = FileJoinCaller.INSTANCE.checkStatus(hhFilePath)
                    && FirstStageNodeFileJoinerCaller.INSTANCE.checkStatus(hhFilePath) && IncrementalDataHandler.INSTANCE.checkStatus(hhFilePath);
            if (status) {
                logger.info("Publish successful for {}",hhFilePath);
                String baseFolderPath = FileSystemContext.getRootDirectory() + hhFilePath;
                String dataFolderPath = baseFolderPath + File.separator + FileSystemContext.getDataFilePrefix()+File.separator;
                String metadataFilePath = baseFolderPath + File.separator + FileSystemConstants.META_DATA_FOLDER_NAME
                        + File.separator + NodeInfo.INSTANCE.getId();
                String fileStatisticsPath = baseFolderPath + File.separator + FileSystemConstants.FILE_STATISTICS_FOLDER_NAME
                        + File.separator + NodeInfo.INSTANCE.getId();
                String blockStatisticsFolderPath = baseFolderPath + File.separator + FileSystemConstants.BLOCK_STATISTICS_FOLDER_NAME
                        + File.separator + NodeInfo.INSTANCE.getId();
                ApplicationCache.INSTANCE.getContext(hhFilePath);
                try{
                    Map<Integer, String> indexToFileNamesV2 = ApplicationCache.INSTANCE.getIndexToFileNamesForFirstDimension(hhFilePath);
                    DataSynchronizer.INSTANCE.synchronize(dataFolderPath, indexToFileNamesV2.values(), metadataFilePath, hhFilePath, fileStatisticsPath, blockStatisticsFolderPath);
                }finally {
                    ApplicationCache.INSTANCE.releaseContext(hhFilePath);
                }
                dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
            } else {
                logger.info("Publish failed for {}",hhFilePath);
                HHFileStatusCoordinator.updateFailure(hhFilePath, "File Joiner failed");
                dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
            }
            dataOutputStream.flush();
        } catch (Exception e) {
            if (hhFilePath != null) {
                HHFileStatusCoordinator.updateFailure(hhFilePath, e.toString());
            }
            try {
                dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
                dataOutputStream.flush();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.gc();
        }
    }

}
