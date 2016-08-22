package com.talentica.hungryHippos.node.tools.handlers;

import com.talentica.hungryHippos.utility.SocketMessages;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * This is a handler class for File extraction request
 * Created by rajkishoreh on 18/8/16.
 */
public class FileExtractionRequestHandler implements RequestHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileExtractionRequestHandler.class);

    @Override
    public void handleRequest(DataInputStream dis, DataOutputStream dos) throws IOException {
        LOGGER.info("[{}] Request handling started",Thread.currentThread().getName());
        try {
            String fileToExtract = dis.readUTF();
            TarAndGzip.untarTGzFile(fileToExtract);
            LOGGER.info("[{}] Successfully extracted : {}",Thread.currentThread().getName(),fileToExtract);
            dos.writeInt(SocketMessages.SUCCESS.ordinal());
        } catch (Exception e) {
            LOGGER.error("[{}] {}",Thread.currentThread().getName(),e.toString());
            dos.writeInt(SocketMessages.FAILED.ordinal());
            e.printStackTrace();
        }
    }
}
