package com.talentica.hungryHippos.storage;

import com.talentica.hungryHippos.utility.MemoryStatus;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by debasishc on 31/8/15.
 */
public class FileDataStore implements DataStore {
    /**
     *
     */
    private static final Logger logger = LoggerFactory.getLogger(FileDataStore.class);
    private Map<String, OutputStream> fileNameToOutputStreamMap;
    private OutputStream[] outputStreams;
    private String hungryHippoFilePath;
    private int nodeId;
    private String dataFilePrefix;

    public FileDataStore(Map<Integer, String> fileNames, int maxBucketSize, int numDimensions,
                         String hungryHippoFilePath, String nodeId,
                         String fileName) throws IOException, InterruptedException, ClassNotFoundException,
            KeeperException, JAXBException {
        this(fileNames, maxBucketSize, numDimensions,  hungryHippoFilePath, nodeId, false,  fileName);
    }

    public FileDataStore(Map<Integer, String> fileNames, int maxBucketSize, int numDimensions,
                         String hungryHippoFilePath, String nodeId, boolean readOnly,
                          String fileName) throws IOException {

        fileNameToOutputStreamMap = new HashMap<>();
        this.nodeId = Integer.parseInt(nodeId);
        this.hungryHippoFilePath = hungryHippoFilePath;
        this.dataFilePrefix = FileSystemContext.getRootDirectory() + hungryHippoFilePath
                + File.separator + fileName;

        int maxFiles = (int) Math.pow(maxBucketSize, numDimensions);
        this.outputStreams = new OutputStream[maxFiles];
        if (!readOnly) {
            File file = new File(dataFilePrefix);
            if (!file.exists()) {
                boolean flag = file.mkdirs();
                if (flag) {
                    logger.info("created data folder");
                } else {
                    logger.info("Not able to create dataFolder");
                }
            }
            dataFilePrefix = dataFilePrefix + "/";
            allocateResources(fileNames,this.outputStreams,this.dataFilePrefix,this.fileNameToOutputStreamMap);

        }
    }

    private static synchronized void allocateResources(Map<Integer, String> fileNames, OutputStream[] outputStreams, String dataFilePrefix, Map<String, OutputStream> fileNameToOutputStreamMap) throws FileNotFoundException {
        long usableMemory = MemoryStatus.getUsableMemory();
        long memoryRequiredForBufferedStream = fileNames.size() * 1024;
        if (usableMemory > memoryRequiredForBufferedStream) {
            for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                outputStreams[entry.getKey()] = new BufferedOutputStream(new FileOutputStream(dataFilePrefix + entry.getValue()), 1024);
                fileNameToOutputStreamMap.put(entry.getValue(), outputStreams[entry.getKey()]);
            }
        } else {
            for (Map.Entry<Integer, String> entry : fileNames.entrySet()) {
                outputStreams[entry.getKey()] = new FileOutputStream(dataFilePrefix + entry.getValue());
                fileNameToOutputStreamMap.put(entry.getValue(), outputStreams[entry.getKey()]);
            }
        }
    }

    @Override
    public void storeRow(String name, byte[] raw) {
        try {
            fileNameToOutputStreamMap.get(name).write(raw);
        } catch (NullPointerException e) {
            logger.error(name + " not present");
        } catch (IOException e) {
            logger.error("Error occurred while writing data received to datastore. {} ", e.toString());
        }
    }

    @Override
    public void storeRow(int index, byte[] raw) {
        try {
            outputStreams[index].write(raw);
        } catch (IOException e) {
            logger.error("Error occurred while writing data received to datastore. {} ", e.toString());
        }
    }

    @Override
    public void sync() {

        long totalSizeOnNode = 0L;
        try {
            for (String name : fileNameToOutputStreamMap.keySet()) {
                try {
                    fileNameToOutputStreamMap.get(name).flush();
                    String filePath = new String(dataFilePrefix + name);
                    File file = new File(filePath);
                    totalSizeOnNode += file.length();
                } catch (IOException e) {
                    logger.error("Error occurred while flushing " + name + " outputstream.", e);
                } finally {
                    try {
                        if (fileNameToOutputStreamMap.get(name) != null)
                            fileNameToOutputStreamMap.get(name).close();

                    } catch (IOException e) {
                        logger.warn("\n\tUnable to close the connection; exception :: " + e.getMessage());
                    }
                }
            }
            HungryHipposFileSystem.getInstance().updateFSBlockMetaData(hungryHippoFilePath, nodeId, totalSizeOnNode);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public String getHungryHippoFilePath() {
        return hungryHippoFilePath;
    }


}
