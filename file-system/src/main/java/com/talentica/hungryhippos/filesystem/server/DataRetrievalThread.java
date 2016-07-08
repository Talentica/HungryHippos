package com.talentica.hungryhippos.filesystem.server;

import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryhippos.filesystem.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.property.FileSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * This class is for creating threads for handling each client request for Data Retrieval
 * Created by rajkishoreh on 30/6/16.
 */
public class DataRetrievalThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataRetrievalThread.class);

    private FileInputStream fis = null;
    private BufferedInputStream bis = null;
    private Socket clientSocket = null;
    private DataInputStream dis = null;
    private DataOutputStream dos = null;
    private Property<FileSystemProperty> fileSystemProperty = null;

    public DataRetrievalThread(Socket clientSocket,Property<FileSystemProperty> fileSystemProperty) {
        this.clientSocket = clientSocket;
        this.fileSystemProperty = fileSystemProperty;
        LOGGER.info("[{}] Just connected to {}", Thread.currentThread().getName(), clientSocket.getRemoteSocketAddress());
    }

    public void run() {
        try {
            dis = new DataInputStream(clientSocket.getInputStream());
            dos = new DataOutputStream(clientSocket.getOutputStream());
            dos.writeUTF(FileSystemConstants.DATA_SERVER_AVAILABLE);
            String fileZKNode = dis.readUTF();
            String dataNodes = dis.readUTF();
            LOGGER.info("[{}] DataNodes : {}", Thread.currentThread().getName(), dataNodes);
            String[] filePathsArr = dataNodes.split(FileSystemConstants.FILE_PATHS_DELIMITER);
            int fileStreamBufferSize = Integer.parseInt(fileSystemProperty.getValueByKey(FileSystemConstants.FILE_STREAM_BUFFER_SIZE));
            byte[] inputBuffer = new byte[fileStreamBufferSize];
            int len;
            for (String filePath : filePathsArr) {
                String absoluteFilePath = fileSystemProperty.getValueByKey(FileSystemConstants.HHROOT) + File.separator +
                        fileZKNode + File.separator +fileSystemProperty.getValueByKey(FileSystemConstants.DATA_FILE_PREFIX) +
                        filePath;
                fis = new FileInputStream(absoluteFilePath);
                bis = new BufferedInputStream(fis);
                LOGGER.info("[{}] Sending Data of : {}", Thread.currentThread().getName(), absoluteFilePath);
                while ((len = bis.read(inputBuffer)) > -1) {
                    dos.write(inputBuffer, 0, len);
                }
                bis.close();
                dos.flush();
            }
            Thread.sleep(1000);
            dos.writeUTF(FileSystemConstants.DATA_TRANSFER_COMPLETED);
            LOGGER.info("[{}] {}", Thread.currentThread().getName(), FileSystemConstants.DATA_TRANSFER_COMPLETED);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
                if (dis != null) {
                    dis.close();
                }
                if (dos != null) {
                    dos.close();
                }
                if (fis != null) {
                    fis.close();
                }
                if (bis != null) {
                    bis.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
