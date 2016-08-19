package com.talentica.hungryHippos.tools.clients;

import com.talentica.hungryHippos.coordination.context.ToolsConfigContext;
import com.talentica.hungryHippos.utility.RequestType;
import com.talentica.hungryHippos.utility.SocketMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * This ia an abstract class for sending Client requests
 * Created by rajkishoreh on 18/8/16.
 */
public abstract class AbstractClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractClient.class);
    private RequestType requestType;

    public AbstractClient(RequestType requestType) {
        this.requestType = requestType;
    }

    /**
     * Sends request to a particular hostIP
     *
     * @param hostIP
     * @throws IOException
     */
    public void sendRequest(String hostIP) throws IOException {
        int i = 0;
        int maxQueryAttempts = ToolsConfigContext.getMaxQueryAttempts();
        int port = ToolsConfigContext.getServerPort();
        long retryTimeInterval = ToolsConfigContext.getQueryRetryInterval();
        for (i = 0; i < maxQueryAttempts; i++) {
            try (Socket client = new Socket(hostIP, port);
                 DataOutputStream dos = new DataOutputStream(client.getOutputStream());
                 DataInputStream dis = new DataInputStream(client.getInputStream())) {
                int serverAvailability = dis.readInt();
                LOGGER.info(SocketMessages.values()[serverAvailability].name());
                if (SocketMessages.SERVER_AVAILABLE.ordinal() == serverAvailability) {
                    dos.writeInt(requestType.ordinal());
                    int requestTypeResponseCode = dis.readInt();
                    LOGGER.info(SocketMessages.values()[requestTypeResponseCode].name());
                    if (SocketMessages.VALID_REQUEST_TYPE.ordinal() == requestTypeResponseCode) {
                        process(dis, dos);
                    } else {
                        LOGGER.error("{} : {}", RequestType.values()[requestTypeResponseCode].name(), requestType.name());
                    }
                } else {
                    LOGGER.info("Retrying after {} milliseconds", retryTimeInterval);
                    Thread.sleep(retryTimeInterval);
                }
            } catch (IOException e) {
                LOGGER.error(e.toString());
            } catch (InterruptedException e) {
                LOGGER.error(e.toString());
            }
        }
        if (i == maxQueryAttempts) {
            throw new RuntimeException("Failed");
        }
    }

    /**
     * This method has to be implemented for various type of requests
     *
     * @param dis
     * @param dos
     * @throws IOException
     */
    public abstract void process(DataInputStream dis, DataOutputStream dos) throws IOException;
}
