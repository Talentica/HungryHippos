package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.datareceiver.MetaDataSynchronizer;
import com.talentica.hungryHippos.node.datareceiver.NewDataHandler;
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

/**
 * Created by rajkishoreh on 6/2/17.
 */
public class MetaDataSynchronizerService implements Runnable{

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
        String hhFilePath= null;
        try {
            hhFilePath = dataInputStream.readUTF();
            String baseFolderPath = FileSystemContext.getRootDirectory() + hhFilePath;
            String dataFolderPath = baseFolderPath + File.separator + FileSystemContext.getDataFilePrefix();
            File dataFolder = new File(dataFolderPath);
            String[] files = dataFolder.list();
            String metadataFilePath = baseFolderPath + File.separator + FileSystemConstants.META_DATA_FOLDER_NAME
                    + File.separator + NodeInfo.INSTANCE.getId();
            MetaDataSynchronizer.INSTANCE.synchronize(dataFolderPath,files,dataFolderPath,metadataFilePath,hhFilePath);
            dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
            dataOutputStream.flush();
        } catch (IOException | InterruptedException e) {
            if (hhFilePath != null) {
                NewDataHandler.updateFailure(hhFilePath, e.toString());
            }
            try {
                dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
                dataOutputStream.flush();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }finally {
            if(socket!=null){
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
