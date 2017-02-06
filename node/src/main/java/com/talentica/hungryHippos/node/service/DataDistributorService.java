package com.talentica.hungryHippos.node.service;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.node.DataDistributorStarter;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.datareceiver.NewDataHandler;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryHippos.utility.MemoryStatus;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by rajkishoreh on 24/11/16.
 */
public class DataDistributorService implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataDistributorService.class);
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    private Socket socket;

    public DataDistributorService(Socket socket) throws IOException {
        this.socket = socket;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        String hhFilePath = null;
        String srcDataPath = null;

        try {
            hhFilePath = dataInputStream.readUTF();
            srcDataPath = dataInputStream.readUTF();

            int idealBufSize = socket.getReceiveBufferSize();
            byte[] buffer;
            if (MemoryStatus.getUsableMemory() > idealBufSize) {
                buffer = new byte[socket.getReceiveBufferSize()];
            } else {
                buffer = new byte[2048];
            }

            int read = 0;
            File file = new File(srcDataPath);
            Path parentDir = Paths.get(file.getParent());
            if (!(Files.exists(parentDir))) {
                Files.createDirectories(parentDir);
            }

            long size = dataInputStream.readLong();

            OutputStream bos =
                    new BufferedOutputStream(new FileOutputStream(srcDataPath));

            while (size != 0) {
                read = dataInputStream.read(buffer);
                bos.write(buffer, 0, read);
                size -= read;
            }
            bos.flush();
            bos.close();
            buffer = null;
            bos = null;
            System.gc();
            dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
            System.out.println("finished reading");
            if (!NewDataHandler.checkIfFailed(hhFilePath)) {
                DataDistributor.distribute(hhFilePath, srcDataPath);
            }
            dataOutputStream.writeUTF(HungryHippoServicesConstants.SUCCESS);
            dataOutputStream.flush();

        } catch (Exception e) {
            if (hhFilePath != null) {
                NewDataHandler.updateFailure(hhFilePath, e.toString());
            }
            try {
                dataOutputStream.writeUTF(HungryHippoServicesConstants.FAILURE);
                dataOutputStream.flush();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        } finally {

            if (srcDataPath != null) {
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

    private void checkPublishStatus(String hhFilePath, String fileIdToHHpath) throws HungryHippoException {
        while (true) {
            HungryHippoCurator curator = HungryHippoCurator.getInstance();
            List<String> children = curator.getChildren(fileIdToHHpath);
            if (children == null || children.isEmpty()) {
                curator.deletePersistentNodeIfExits(fileIdToHHpath);
                break;
            }
            if (NewDataHandler.checkIfFailed(hhFilePath)) {
                throw new RuntimeException("File distribution failed for " + hhFilePath + " in " + NodeInfo.INSTANCE.getIp());
            }
        }
    }

    private static int fileIdToHHPathMap(String path, String inputHHPath) {
        int i = 0;
        while (true) {
            try {
                HungryHippoCurator.getInstance().createPersistentNode(path + i, inputHHPath);
                return i;
            } catch (HungryHippoException e) {
                if (e instanceof HungryHippoException) {
                    i++;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
