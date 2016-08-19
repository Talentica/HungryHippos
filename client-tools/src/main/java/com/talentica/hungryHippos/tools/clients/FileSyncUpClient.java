package com.talentica.hungryHippos.tools.clients;

import com.talentica.hungryHippos.utility.RequestType;
import com.talentica.hungryHippos.utility.SocketMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * This class is for sending request for file sync up
 * Created by rajkishoreh on 18/8/16.
 */
public class FileSyncUpClient extends AbstractClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSyncUpClient.class);

    private String seedFilePath;

    public FileSyncUpClient(RequestType requestType, String seedFilePath) {
        super(requestType);
        this.seedFilePath = seedFilePath;
    }

    @Override
    public void process(DataInputStream dis, DataOutputStream dos) throws IOException {
        dos.writeUTF(seedFilePath);
        int status = dis.readInt();
        if (SocketMessages.SUCCESS.ordinal() == status) {
            LOGGER.info("File Sync Up successful for : " + seedFilePath);
        } else {
            LOGGER.error("File Sync Up failed for : " + seedFilePath);
            throw new RuntimeException("File Sync Up failed for : " + seedFilePath);
        }
    }
}
