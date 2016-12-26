package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.datareceiver.NewDataHandler;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import org.apache.commons.io.FileUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;

/**
 * Created by rajkishoreh on 24/11/16.
 */
public class DataDistributorService implements Runnable {


    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;

    public DataDistributorService(Socket socket) throws IOException {
        this.socket = socket;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
    }

    @Override
    public void run(){
        String hhFilePath = null;
        String srcDataPath = null;
        try {
            hhFilePath = dataInputStream.readUTF();
            srcDataPath = dataInputStream.readUTF();
            if(!NewDataHandler.checkIfFailed(hhFilePath)){
                DataDistributor.distribute(hhFilePath,srcDataPath);
            }
            dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
            dataOutputStream.flush();
        } catch (Exception e) {
            if(hhFilePath!=null){
                NewDataHandler.updateFailure(hhFilePath, NodeInfo.INSTANCE.getIp()+" : "+e.toString());
            }
            try {
                dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
                dataOutputStream.flush();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
        finally {
            if(srcDataPath!=null){
                try {
                    FileUtils.deleteDirectory((new File(srcDataPath)).getParentFile());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
