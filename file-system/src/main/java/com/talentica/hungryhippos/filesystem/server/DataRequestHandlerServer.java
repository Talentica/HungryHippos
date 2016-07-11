package com.talentica.hungryhippos.filesystem.server;

import com.talentica.hungryhippos.filesystem.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This class is for creating a Server which listens at a particular port
 * and creates Threads for handling client requests
 * <p>
 * Created by rajkishoreh on 30/6/16.
 */
public class DataRequestHandlerServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataRequestHandlerServer.class);

    private int port;
    private int maximumClientRequests;
    private DataRetrievalThread[] requestHandlingThreads;

    public DataRequestHandlerServer(int port, int maximumClientRequests) {
        this.port = port;
        this.maximumClientRequests = maximumClientRequests;
        this.requestHandlingThreads = new DataRetrievalThread[maximumClientRequests];
    }

    /**
     * This method starts a server which creates Threads for clients requests coming for Data Retrieval
     *
     * @throws IOException
     */
    public void start() throws IOException, InterruptedException, ClassNotFoundException, KeeperException, JAXBException {

        ServerSocket serverSocket = new ServerSocket(port);
        while (true) {
            try {
                LOGGER.info("[{}] Waiting for client on port {} ...", Thread.currentThread().getName(), serverSocket.getLocalPort());
                Socket clientSocket = serverSocket.accept();
                int i = 0;
                for (i = 0; i < maximumClientRequests; i++) {
                    if (requestHandlingThreads[i] == null || !requestHandlingThreads[i].isAlive()) {
                        LOGGER.info("[{}] Assigning slot {}", Thread.currentThread().getName(), i);
                        (requestHandlingThreads[i] = new DataRetrievalThread(clientSocket,FileSystemContext.getRootDirectory(),FileSystemContext.getDataFilePrefix(),FileSystemContext.getFileStreamBufferSize())).start();
                        break;
                    }
                }
                if (i == maximumClientRequests) {
                    LOGGER.info("[{}] Server too busy. Try Later", Thread.currentThread().getName());
                    DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
                    dos.writeUTF(FileSystemConstants.DATA_SERVER_BUSY);
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * This is the main method to start the DataRequestHandlerServer
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, KeeperException, JAXBException {
        int port = FileSystemContext.getServerPort();
        int maximumClientRequests = FileSystemContext.getMaxClientRequests();
        new DataRequestHandlerServer(port, maximumClientRequests).start();
    }
}