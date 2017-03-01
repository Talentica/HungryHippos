package com.talentica.hungryHippos.node.service;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.UUID;

import com.talentica.hungryHippos.node.DataDistributorStarter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.node.datareceiver.FileJoiner;
import com.talentica.hungryHippos.node.datareceiver.NewDataHandler;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryHippos.utility.scp.TarAndUntar;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * Created by rajkishoreh on 20/12/16.
 */
public class DataAppenderService implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(DataAppenderService.class);
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;

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
            int noOfRemainingAttempts = 25;
            while(noOfRemainingAttempts > 0){
              try{
                TarAndUntar.untar(srcTarFilePath, srcFolderPath);
                break;
              }catch(IOException e){
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
                noOfRemainingAttempts--;
                logger.error("[{}] Retrying File untar for {}", Thread.currentThread().getName(), srcFolderPath);
              }
            }
            srcTarFile.delete();
            if(noOfRemainingAttempts == 0){
              dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
              logger.info("[{}] unable to untar {} , File exists status {}",Thread.currentThread().getName(),srcTarFilePath);
            }else{
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
            DataDistributorStarter.cacheClearServices.execute(new CacheClearService());
        }

    }
}
