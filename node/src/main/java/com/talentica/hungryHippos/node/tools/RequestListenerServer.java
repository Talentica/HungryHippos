package com.talentica.hungryHippos.node.tools;


import com.talentica.hungryHippos.coordination.context.ToolsConfigContext;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.SocketMessages;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This class is for listening requests from client assigning a thread
 * Created by rajkishoreh on 18/8/16.
 */
public class RequestListenerServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RequestListenerServer.class);

    private int port;
    private int maximumClientRequests;
    private RequestHandlerThread[] requestThreads;

    public RequestListenerServer(int port, int maximumClientRequests) {
        this.port = port;
        this.maximumClientRequests = maximumClientRequests;
        this.requestThreads = new RequestHandlerThread[maximumClientRequests];
    }

    /**
     * This is the main method to start the RequestListenerServer
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws KeeperException
     * @throws JAXBException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, KeeperException, JAXBException {
        LOGGER.info("Starting RequestListenerServer");
        validateArguments(args);
        NodesManagerContext.getNodesManagerInstance(args[0]);
        int port = ToolsConfigContext.getServerPort();
        int maximumClientRequests = ToolsConfigContext.getMaxClientRequests();
        new RequestListenerServer(port, maximumClientRequests).start();
    }

    private static void validateArguments(String[] args) {
        if (args.length < 1) {
            throw new RuntimeException("Please provide client-config.xml to connect to zookeeper.");
        }
    }

    /**
     * This method starts a server which creates Threads for clients requests
     *
     * @throws IOException
     */
    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        while (true){
            try {
                LOGGER.info("[{}] Waiting for client on port {} ...", Thread.currentThread().getName(), serverSocket.getLocalPort());
                Socket clientSocket = serverSocket.accept();
                int i = 0;
                for (i = 0; i < maximumClientRequests; i++) {
                    if (requestThreads[i] == null || !requestThreads[i].isAlive()) {
                        LOGGER.info("[{}] Assigning slot {}", Thread.currentThread().getName(), i);
                        (requestThreads[i] = new RequestHandlerThread(clientSocket)).start();
                        break;
                    }
                }
                if (i == maximumClientRequests) {
                    LOGGER.info("[{}] Server too busy. Try Later", Thread.currentThread().getName());
                    DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
                    dos.writeInt(SocketMessages.SERVER_BUSY.ordinal());
                    clientSocket.close();
                }
            } catch (IOException e) {
                LOGGER.error("[{}] {}",Thread.currentThread().getName(),e.toString());
                throw new RuntimeException(e);
            }
        }
    }


}
