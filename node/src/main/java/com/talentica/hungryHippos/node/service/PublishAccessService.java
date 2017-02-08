package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.DataDistributorStarter;
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
public class PublishAccessService implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(PublishAccessService.class);
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;

    public PublishAccessService(Socket socket) throws IOException {
        this.socket = socket;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
    }


    @Override
    public void run() {
        logger.info("Lock access provided to {}", this.socket.getInetAddress());
        try {
            int count = DataDistributorStarter.noOfAvailableDataDistributors.decrementAndGet();
            if(count<0){
                DataDistributorStarter.noOfAvailableDataDistributors.incrementAndGet();
                this.dataOutputStream.writeBoolean(false);
            }else{
                this.dataOutputStream.writeBoolean(true);
                DataDistributorStarter.dataDistributorService.execute(new DataDistributorService(socket));
            }
            logger.info("Lock released from {}", this.socket.getInetAddress());
        } catch (IOException e) {
            e.printStackTrace();

        }
    }
}
