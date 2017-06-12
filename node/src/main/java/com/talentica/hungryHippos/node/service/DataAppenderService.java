package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.DataDistributorStarter;
import com.talentica.hungryHippos.node.datareceiver.HHFileStatusCoordinator;
import com.talentica.hungryHippos.node.joiners.CallableGenerator;
import com.talentica.hungryHippos.node.joiners.FileJoinCaller;
import com.talentica.hungryHippos.node.joiners.TarFileJoiner;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.UUID;

/**
 * Created by rajkishoreh on 4/5/17.
 */
public class DataAppenderService implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(DataAppenderService.class);
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;
    private CallableGenerator callableGenerator;

    public DataAppenderService(Socket socket, CallableGenerator callableGenerator) throws IOException {
        this.socket = socket;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
        this.callableGenerator = callableGenerator;
    }

    @Override
    public void run() {
        File srcFolder = null;
        String srcFolderPath = null;
        String hhFilePath = null;
        try {
            hhFilePath = dataInputStream.readUTF();
            srcFolderPath = FileSystemContext.getRootDirectory() + hhFilePath + File.separator + UUID.randomUUID().toString();
            srcFolder = new File(srcFolderPath);
            srcFolder.mkdirs();
            String srcTarFileName = dataInputStream.readUTF();
            String srcTarFilePath = srcFolderPath + File.separator + srcTarFileName;
            File srcTarFile = new File(srcTarFilePath);
            String destFolderPath = dataInputStream.readUTF();
            long fileSize = dataInputStream.readLong();
            int bufferSize = 2048;
            byte[] buffer = new byte[bufferSize];
            FileOutputStream fos= new FileOutputStream(srcTarFile);
            BufferedOutputStream bos = new BufferedOutputStream(fos, 10 * bufferSize);
            int len;
            while (fileSize > 0) {
                len = dataInputStream.read(buffer);
                bos.write(buffer, 0, len);
                fileSize = fileSize - len;
            }
            bos.flush();
            fos.flush();
            bos.close();
            fos.close();
            logger.info("[{}] marking {} for {}", Thread.currentThread().getName(), srcFolderPath, destFolderPath);
            FileJoinCaller.INSTANCE.addSrcFile(hhFilePath,srcTarFilePath, callableGenerator);
            dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
            logger.info("[{}] Successfully marked {} for {}", Thread.currentThread().getName(), srcFolderPath, destFolderPath);
            dataOutputStream.flush();
        } catch (IOException e) {
            try {
                dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
                dataOutputStream.flush();
                if (hhFilePath != null) {
                    HHFileStatusCoordinator.updateFailure(hhFilePath, e.getMessage());
                }
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            DataDistributorStarter.cacheClearServices.execute(new CacheClearService());
        }

    }
}
