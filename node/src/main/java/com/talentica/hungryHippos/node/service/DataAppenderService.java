package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.DataDistributorStarter;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.datareceiver.FileJoiner;
import com.talentica.hungryHippos.node.datareceiver.NewDataHandler;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * Created by rajkishoreh on 20/12/16.
 */
public class DataAppenderService implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(DataAppenderService.class);
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;
    private static final String SCRIPT_FOR_UNTAR_AND_REMOVE_SRC="untar-file-and-remove-src.sh";

    public DataAppenderService(Socket socket) throws IOException {
        this.socket = socket;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        File srcFolder = null;
        String srcFolderPath = null;
        String hhFilePath = null;
        try {
            hhFilePath = dataInputStream.readUTF();
            srcFolderPath = dataInputStream.readUTF();
            srcFolder = new File(srcFolderPath);
            srcFolder.mkdirs();
            String srcTarFileName = dataInputStream.readUTF();
            String srcTarFilePath = srcFolderPath+File.separator+srcTarFileName;
            File srcTarFile = new File(srcTarFilePath);
            String destFolderPath = dataInputStream.readUTF();
            long fileSize = dataInputStream.readLong();
            int bufferSize = 2048;
            byte[] buffer = new byte[bufferSize];
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(srcTarFile),10*bufferSize);
            int len;
            while(fileSize>0){
                len=dataInputStream.read(buffer);
                bos.write(buffer,0,len);
                fileSize = fileSize-len;
            }
            bos.flush();
            bos.close();
            Process untarProcess = Runtime.getRuntime().exec(System.getProperty("hh.bin.dir")+SCRIPT_FOR_UNTAR_AND_REMOVE_SRC+" "+srcFolderPath+" "+srcTarFileName);
            int untarProcessStatus = untarProcess.waitFor();
            String line;
            if(untarProcessStatus!=0){
                BufferedReader br = new BufferedReader(new InputStreamReader(untarProcess.getErrorStream()));
                while ((line = br.readLine()) != null) {
                    logger.error(line);
                }
                br.close();
                dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
                logger.info("[{}] unable to untar {} , File exists status {}",Thread.currentThread().getName(),srcTarFilePath);
            }else{
                logger.info("[{}] joining {} into {}",Thread.currentThread().getName(),srcFolderPath,destFolderPath);
                String lockString = destFolderPath+socket.getInetAddress();
                FileJoiner.INSTANCE.join(srcFolderPath, destFolderPath, lockString);
                dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
                logger.info("[{}] Successfully joined {} into {}",Thread.currentThread().getName(),srcFolderPath,destFolderPath);
            }
            dataOutputStream.flush();

        } catch (IOException | InterruptedException e) {
            try {
                dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
                dataOutputStream.flush();
                if(hhFilePath!=null){
                    NewDataHandler.updateFailure(hhFilePath, e.getMessage());
                }
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            try {
                if (srcFolder != null) {
                    if (srcFolder.exists()) {
                        logger.info("[{}] Deleting {} ",Thread.currentThread().getName(),srcFolderPath);
                        FileUtils.deleteDirectory(srcFolder);
                    }
                }
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
