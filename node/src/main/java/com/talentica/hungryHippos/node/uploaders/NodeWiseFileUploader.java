package com.talentica.hungryHippos.node.uploaders;

import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rajkishoreh on 15/5/17.
 */
public class NodeWiseFileUploader extends AbstractFileUploader {

    public NodeWiseFileUploader(CountDownLatch countDownLatch, String srcFolderPath, String destinationPath,
                              int idx, Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, Node node,
                              Set<String> fileNames, String hhFilePath, String fileName) {
        super(countDownLatch,srcFolderPath,destinationPath,idx,dataInputStreamMap, socketMap,node,fileNames,hhFilePath, fileName);
    }

    @Override
    protected void createTar(String tarFilename) throws IOException {

    }

    @Override
    public void writeAppenderType(DataOutputStream dos) throws IOException {
        dos.writeInt(HungryHippoServicesConstants.NODE_DATA_APPENDER);
    }
}
