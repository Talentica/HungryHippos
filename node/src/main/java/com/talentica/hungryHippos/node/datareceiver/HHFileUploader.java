package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.node.DataReceiver;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryhippos.config.cluster.Node;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rajkishoreh on 25/11/16.
 */
public class HHFileUploader {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileJoiner.class);

    private static Map<String, CountDownLatch> countDownMap = new ConcurrentHashMap<>();


    public static final String SCRIPT_FOR_FILE_TRANSFER = "transfer-files.sh";

    private static Object classLock = new Object();


    public static void uploadFile(String srcFolderPath, String hhFilePath, Map<Integer, Set<String>> nodeToFileMap) throws IOException, InterruptedException {
        LOGGER.info("Inside uploadFile for {}", hhFilePath);

        List<Node> nodes = CoordinationConfigUtil.getZkClusterConfigCache().getNode();
        CountDownLatch countDownLatch = countDownMap.get(hhFilePath);
        if (countDownLatch == null) {
            synchronized (classLock) {
                countDownLatch = countDownMap.get(hhFilePath);
                if (countDownLatch == null) {
                    countDownLatch = new CountDownLatch(nodes.size());
                    countDownMap.put(hhFilePath, countDownLatch);
                }
            }
        }

        String tarFileName = NodeInfo.INSTANCE.getId();
        String remoteTargetFolder = srcFolderPath;
        String sshUserName = DataReceiver.getUserName();
        String hungryHippoBinDir = System.getProperty("hh.bin.dir");
        synchronized (countDownLatch) {
            countDownLatch.countDown();
            if (countDownLatch.getCount() == 0) {
                LOGGER.info("Sending Replica Data To Nodes for {}", hhFilePath);
                String commonCommandArg = hungryHippoBinDir + SCRIPT_FOR_FILE_TRANSFER + " " + srcFolderPath + " " + tarFileName + " " + remoteTargetFolder + " " + sshUserName;
                String line = "";
                for (com.talentica.hungryhippos.config.cluster.Node node : nodes) {
                    Set<String> fileNames = nodeToFileMap.get(node.getIdentifier());
                    if (fileNames != null && !fileNames.isEmpty()) {
                        String fileNamesArg = StringUtils.join(fileNames, " ");
                        Process process = Runtime.getRuntime().exec(commonCommandArg + " " + node.getIp() + " " + fileNamesArg);
                        int processStatus = process.waitFor();
                        if (processStatus != 0) {
                            BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                            while ((line = br.readLine()) != null) {
                                LOGGER.error(line);
                            }
                            br.close();
                            throw new RuntimeException("File transfer failed");
                        }
                    }
                }
                countDownMap.remove(hhFilePath);
                LOGGER.info("Completed Sending Replica Data To Nodes for {}", hhFilePath);
            }else{
                LOGGER.info("Upload will be done after receiving remaining {} chunks for {}",countDownLatch.getCount(), hhFilePath);
            }
        }
    }
}
