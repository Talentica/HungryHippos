package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Created by rajkishoreh on 21/12/16.
 */
public class SCPAccessService implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(SCPAccessService.class);
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;

    public SCPAccessService(Socket socket) throws IOException {
        this.socket = socket;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
    }


    @Override
    public void run() {
        logger.info("Lock access provided to {}", this.socket.getInetAddress());
        try {
            this.dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
            boolean releaseFlag = this.dataInputStream.readBoolean();
            logger.info("Lock released from {}", this.socket.getInetAddress());
        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            try {
                this.socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
