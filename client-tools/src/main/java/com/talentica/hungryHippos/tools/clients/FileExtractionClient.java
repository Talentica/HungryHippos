package com.talentica.hungryHippos.tools.clients;

import com.talentica.hungryHippos.utility.RequestType;
import com.talentica.hungryHippos.utility.SocketMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * This class is for sending request for file extraction
 * Created by rajkishoreh on 18/8/16.
 */
public class FileExtractionClient extends AbstractClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileExtractionClient.class);
    private String filePath;

    public FileExtractionClient(String filePath) {
        super(RequestType.EXTRACT_FILE);
        this.filePath = filePath;
    }

    @Override
    public void process(DataInputStream dis, DataOutputStream dos) throws IOException {
        dos.writeUTF(filePath);
        int status = dis.readInt();
        if (SocketMessages.SUCCESS.ordinal() == status) {
            LOGGER.info("File Extraction successful for : " + filePath);
        } else {
            LOGGER.error("File Extraction failed for : " + filePath);
            throw new RuntimeException("File Extraction failed for : " + filePath);
        }
    }
}
