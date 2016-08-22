package com.talentica.hungryHippos.node.tools;

import com.talentica.hungryHippos.node.tools.handlers.FileExtractionRequestHandler;
import com.talentica.hungryHippos.node.tools.handlers.FileSyncUpRequestHandler;
import com.talentica.hungryHippos.node.tools.handlers.RequestHandler;
import com.talentica.hungryHippos.utility.RequestType;
import com.talentica.hungryHippos.utility.SocketMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * This class is for getting the appropriate handler and handling the request
 * Created by rajkishoreh on 19/8/16.
 */
public class RequestHandlerThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestHandlerThread.class);

    private Socket socket;

    public RequestHandlerThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            LOGGER.info("[{}] {}",Thread.currentThread().getName(),SocketMessages.SERVER_AVAILABLE.name());
            dos.writeInt(SocketMessages.SERVER_AVAILABLE.ordinal());
            int requestTypeInt = dis.readInt();
            RequestType requestType =  RequestType.values()[requestTypeInt];
            RequestHandler requestHandler = getHandler(requestType);
            if(requestHandler!=null){
                LOGGER.info("[{}] {}",Thread.currentThread().getName(),SocketMessages.VALID_REQUEST_TYPE.name());
                dos.writeInt(SocketMessages.VALID_REQUEST_TYPE.ordinal());
                requestHandler.handleRequest(dis,dos);
            } else{
                LOGGER.info("[{}] {}",Thread.currentThread().getName(),SocketMessages.INVALID_REQUEST_TYPE.name());
                dos.writeInt(SocketMessages.INVALID_REQUEST_TYPE.ordinal());
            }
        } catch (IOException e) {
            LOGGER.error("[{}] {}",Thread.currentThread().getName(),e.toString());
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                LOGGER.error("[{}] {}",Thread.currentThread().getName(),e.toString());
                e.printStackTrace();
            }
        }
    }

    /**
     * Returns handler for the particular requestType
     * @param requestType
     * @return
     */
    private RequestHandler getHandler(RequestType requestType) {
        switch (requestType){
            case EXTRACT_FILE:
                return new FileExtractionRequestHandler();
            case SYNC_UP_FILE:
                return new FileSyncUpRequestHandler();
            default:
                return null;
        }
    }
}
