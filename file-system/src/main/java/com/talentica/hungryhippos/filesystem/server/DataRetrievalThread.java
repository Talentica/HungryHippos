package com.talentica.hungryhippos.filesystem.server;

import com.talentica.hungryhippos.filesystem.FileSystemConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * This class is for creating threads for handling each client request for Data
 * Retrieval Created by rajkishoreh on 30/6/16.
 */
public class DataRetrievalThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataRetrievalThread.class);

    private Socket clientSocket = null;
    private DataInputStream dis = null;
    private DataOutputStream dos = null;
    private String rootDirectory;
    private int fileStreamBufferSize;

    public DataRetrievalThread(Socket clientSocket, String rootDirectory,
                               int fileStreamBufferSize) {
        this.clientSocket = clientSocket;
        this.rootDirectory = rootDirectory;
        this.fileStreamBufferSize = fileStreamBufferSize;
        LOGGER.info("[{}] Just connected to {}", Thread.currentThread().getName(),
                clientSocket.getRemoteSocketAddress());
    }

    public void run() {
        try {
            dis = new DataInputStream(clientSocket.getInputStream());
            dos = new DataOutputStream(clientSocket.getOutputStream());
            dos.writeUTF(FileSystemConstants.DATA_SERVER_AVAILABLE);
            String filePath = dis.readUTF();
            long offset = dis.readLong();
            LOGGER.info("[{}] filePath {}", Thread.currentThread().getName(),filePath);
            LOGGER.info("[{}] offset {}", Thread.currentThread().getName(),offset);
            byte[] inputBuffer = new byte[fileStreamBufferSize];
            int len;
            String absoluteFilePath = rootDirectory + File.separator + filePath;
            RandomAccessFile raf = new RandomAccessFile(absoluteFilePath, "r");
            if (offset < raf.length()) {
                raf.seek(offset);
                while ((len = raf.read(inputBuffer)) > -1) {
                    dos.write(inputBuffer, 0, len);
                }
            }
            raf.close();
            Thread.sleep(1000);
            dos.writeUTF(FileSystemConstants.DATA_TRANSFER_COMPLETED);
            LOGGER.info("[{}] {}", Thread.currentThread().getName(), FileSystemConstants.DATA_TRANSFER_COMPLETED);
        } catch (IOException e) {
            LOGGER.error(e.toString());
        } catch (InterruptedException e) {
            LOGGER.error(e.toString());
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
            } catch (IOException e) {
                LOGGER.error(e.toString());
            }
        }

    }
}
