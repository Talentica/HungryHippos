package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.DataDistributorStarter;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.datareceiver.FileJoiner;
import com.talentica.hungryHippos.node.datareceiver.NewDataHandler;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.UUID;

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
            srcFolderPath = FileSystemContext.getRootDirectory()+hhFilePath+File.separator+ UUID.randomUUID().toString();
            srcFolder = new File(srcFolderPath);
            srcFolder.mkdirs();
            String srcTarFileName = dataInputStream.readUTF();
            String srcTarFilePath = srcFolderPath+File.separator+srcTarFileName;
            File srcTarFile = new File(srcTarFilePath);
            srcTarFile.deleteOnExit();
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
            int untarProcessStatus=-1;
            int noOfRemainingAttempts = 25;
            while(noOfRemainingAttempts>0&&untarProcessStatus<0){
                File[] files = srcFolder.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File pathname) {
                        if(pathname.getAbsolutePath().contains(srcTarFileName)){
                            return false;
                        }
                        return true;
                    }
                });
                for (int i = 0; i < files.length; i++) {
                    files[i].delete();
                }
                Process untarProcess = Runtime.getRuntime().exec(System.getProperty("hh.bin.dir")+SCRIPT_FOR_UNTAR_AND_REMOVE_SRC+" "+srcFolderPath+" "+srcTarFileName);
                untarProcessStatus = untarProcess.waitFor();
                String line;
                if(untarProcessStatus!=0){
                    BufferedReader br = new BufferedReader(new InputStreamReader(untarProcess.getErrorStream()));
                    while ((line = br.readLine()) != null) {
                        logger.error(line);
                    }
                    br.close();
                    br = new BufferedReader(new InputStreamReader(untarProcess.getInputStream()));
                    while ((line = br.readLine()) != null) {
                        logger.info(line);
                    }
                    br.close();
                    noOfRemainingAttempts--;
                    logger.error("[{}] Retrying File untar for {} after 5 seconds", Thread.currentThread().getName(), srcFolderPath);
                    Thread.sleep(5000);
                }
            }
            srcTarFile.delete();
            if(untarProcessStatus<0){
                dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
                logger.info("[{}] unable to untar {} , File exists status {}",Thread.currentThread().getName(),srcTarFilePath);
            }
            else{
                logger.info("[{}] joining {} into {}",Thread.currentThread().getName(),srcFolderPath,destFolderPath);
                String lockString = destFolderPath;
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
