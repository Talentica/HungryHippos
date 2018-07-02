/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryHippos.rdd;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.CustomByteArrayPool;
import com.talentica.hungryhippos.filesystem.FileInfo;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;

public class HHFSClient {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    private File tmpDir;
    private Map<Integer, Node> nodeMap;
    private String downloadFolderPath;
    private String actualRootPath;
    private Random random;
    private Map<String, String> downloadMap;
    private static final String EXTENSION = ".snappy.orc";
    private Map<String,Map<String, int[]>> fileNameToNodeIdCache;
    private Map<String,Map<String, FileInfo>> fileNameToFileInfoCache;


    public HHFSClient() {

        int tries = 4;
        RuntimeException re = null;
        while (tries > 0) {
            try {
                this.actualRootPath = FileSystemContext.getRootDirectory();
                break;
            } catch (RuntimeException e) {
                tries--;
                re = e;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
        if (tries == 0) {
            throw re;
        }

        this.tmpDir = new File(System.getProperty("java.io.tmpdir"));

        this.downloadFolderPath = tmpDir.getAbsolutePath() + File.separator + "HH_TMP_FILES" + UUID.randomUUID().toString() + File.separator;
        File downLoadDir = new File(downloadFolderPath);
        downLoadDir.delete();
        while (!downLoadDir.exists()) {
            downLoadDir.mkdirs();
            downLoadDir.deleteOnExit();
        }
        this.nodeMap = new HashMap<>();
        ClusterConfig clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();
        List<Node> nodes = clusterConfig.getNode();
        for (Node node : nodes) {
            nodeMap.put(node.getIdentifier(), node);
        }
        this.downloadMap = new HashMap<>();
        this.fileNameToNodeIdCache = new HashMap<>();
        this.fileNameToFileInfoCache = new HashMap<>();
        this.random = new Random();
    }

    public Map<String, FileInfo> getFileInfoMetaData(String distributedPath){
        Map<String,FileInfo> fileNameToFileInfo = fileNameToFileInfoCache.get(distributedPath);
        if(fileNameToFileInfo==null) {
            fileNameToFileInfo = readFileInfoMetaData(distributedPath);
        }
        return fileNameToFileInfo;
    }

    public synchronized Map<String,FileInfo> readFileInfoMetaData(String distributedPath) {

        Map<String,FileInfo> fileNameToFileInfo = fileNameToFileInfoCache.get(distributedPath);
        if(fileNameToFileInfo==null) {
            String path = actualRootPath
                    + distributedPath + File.separator + FileSystemConstants.META_DATA_FOLDER_NAME;
            fileNameToFileInfo = new HashMap<>();
            File metadataFolder = new File(path);
            File[] children = metadataFolder.listFiles();
            for (int i = 0; i < children.length; i++) {
                try (FileInputStream fileInputStream = new FileInputStream(children[i]);
                     ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
                    Map<String, FileInfo> tmp = (Map<String, FileInfo>) objectInputStream.readObject();
                    fileNameToFileInfo.putAll(tmp);
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            fileNameToFileInfoCache.put(distributedPath,fileNameToFileInfo);
        }

        return fileNameToFileInfo;

    }

    public Map<String,int[]> getLocationMetaData(String distributedPath){
        Map<String,int[]> fileNameToNodeId = fileNameToNodeIdCache.get(distributedPath);
        if(fileNameToNodeId==null) {
            fileNameToNodeId = readLocationMetaData(distributedPath);
        }
        return fileNameToNodeId;
    }

    public synchronized Map<String,int[]> readLocationMetaData(String distributedPath) {

        Map<String,int[]> fileNameToNodeId = fileNameToNodeIdCache.get(distributedPath);
        if(fileNameToNodeId==null) {
            String path = actualRootPath
                    + distributedPath + File.separator + FileSystemConstants.FILE_LOCATION_INFO;
            try (FileInputStream fileInputStream = new FileInputStream(path);
                 ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
                fileNameToNodeId = (Map<String, int[]>) objectInputStream.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            fileNameToNodeIdCache.put(distributedPath,fileNameToNodeId);
        }

        return fileNameToNodeId;
    }

    public FileStatus getFileStatus(Path path, String distributedPath) {
        String parentPath = path.getParent().getParent().toString();
        String nodeIdKey = path.getParent().toString().replaceFirst(parentPath, "").substring(1);
        String fileInfoKey = path.toString().replaceFirst(parentPath, "").substring(1);
        Map<String, int[]> fileNameToNodeId = getLocationMetaData(distributedPath);
        Map<String, FileInfo> fileNameToFileInfo = getFileInfoMetaData(distributedPath);
        FileInfo metaData = fileNameToFileInfo.get(fileInfoKey);
        if(metaData==null){
            return null;
        }
        int numOfReplicas = fileNameToNodeId.get(nodeIdKey).length;
        String[] names = new String[numOfReplicas];
        String[] hosts = new String[numOfReplicas];
        for (int i = 0; i < numOfReplicas; i++) {
            Node node = nodeMap.get(fileNameToNodeId.get(nodeIdKey)[i]);
            names[i] = node.getName();
            hosts[i] = node.getIp();
        }
        BlockLocation blockLocation = new BlockLocation(names, hosts, 0,
                metaData.length());
        BlockLocation[] blockLocations = new BlockLocation[]{blockLocation};
        LocatedFileStatus locatedFileStatus = new LocatedFileStatus(metaData.length(), false,
                hosts.length,
                metaData.length(), metaData.lastModified(), metaData.lastModified(),
                null, null, null,
                null, path,
                blockLocations);
        return locatedFileStatus;
    }

    public String downloadFile(String actualFilePath, String distributedPath, String nodeIdKey) throws IOException {

        // logger.info("download file {} nodeIdKey {}",actualFilePath,nodeIdKey);
        Map<String, int[]> fileNameToNodeId = getLocationMetaData(distributedPath);
        int numOfReplicas = fileNameToNodeId.get(nodeIdKey).length;
        boolean downloadRequired = true;
        String uniquePath = this.downloadFolderPath + UUID.randomUUID().toString()+EXTENSION;

        IOException exception = null;
        int randomIdx = this.random.nextInt(numOfReplicas);
        for (int i = 0; i < numOfReplicas && downloadRequired; i++) {
            Node node = nodeMap.get(fileNameToNodeId.get(nodeIdKey)[(randomIdx + i) % numOfReplicas]);

            InetSocketAddress inetSocketAddress =
                    new InetSocketAddress(node.getIp(), Integer.valueOf(node.getPort()));

            try {
                Socket socket = new Socket();
                socket.connect(inetSocketAddress, 10000);
                try (DataInputStream dis = new DataInputStream(socket.getInputStream());
                     DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {
                    dos.writeInt(HungryHippoServicesConstants.FILE_PROVIDER);
                    dos.writeBoolean(true);
                    dos.writeUTF(actualFilePath);
                    File file = new File(uniquePath);
                    file.createNewFile();
                    dos.flush();
                    long fileSize = dis.readLong();
                    int bufferSize = (int)Math.min(8192L,fileSize);
                    try(FileOutputStream fos = new FileOutputStream(file);
                    BufferedOutputStream bos = new BufferedOutputStream(fos);){
                        int len = 0;
                        logger.info("Downloading {} of size {} from {}", actualFilePath, fileSize, node.getIp());
                        byte[] buffer = CustomByteArrayPool.INSTANCE.acquireByteArray(bufferSize);
                        //byte[] buffer = new byte[4096];
                        while (fileSize > 0) {
                            len = dis.read(buffer);
                            if(len!=0){
                                bos.write(buffer, 0, len);
                                fileSize -= len;
                            }
                        }
                        dos.writeBoolean(false);
                        bos.flush();
                        fos.flush();
                        CustomByteArrayPool.INSTANCE.releaseByteArray(buffer);
                    }
                    file.deleteOnExit();
                    dos.flush();
                }
                socket.close();
                downloadRequired = false;
            } catch (IOException e) {
                exception = e;
            }
        }
        if (downloadRequired) {
            throw exception;
        }
        this.downloadMap.put(actualFilePath, uniquePath);
        return uniquePath;
    }

    /*public int remoteRead(String actualFilePath, String nodeIdKey, long position, byte[] buf, int off, int len) throws IOException {
        int numOfReplicas = fileNameToNodeId.get(nodeIdKey).length;
        boolean downloadRequired = true;
        IOException exception = null;
        int randomIdx = this.random.nextInt(numOfReplicas);
        for (int i = 0; i < numOfReplicas && downloadRequired; i++) {
            Node node = nodeMap.get(fileNameToNodeId.get(nodeIdKey)[(randomIdx + i) % numOfReplicas]);
            InetSocketAddress inetSocketAddress =
                    new InetSocketAddress(node.getIp(), Integer.valueOf(node.getPort()));
            try {
                Socket socket = new Socket();
                socket.connect(inetSocketAddress, 10000);
                try (DataInputStream dis = new DataInputStream(socket.getInputStream());
                     DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {
                    dos.writeInt(HungryHippoServicesConstants.REMOTE_FILE_READER);
                    dos.writeUTF(actualFilePath);
                    dos.writeLong(position);
                    dos.writeInt(len);
                    dos.flush();
                    int tmpLen = len;
                    int offset = off;
                    while (tmpLen > 0) {
                        int length = dis.read(buf, offset, tmpLen);
                        tmpLen -= length;
                        offset += length;
                    }
                    dos.writeBoolean(true);
                    dos.flush();
                }
                socket.close();
                downloadRequired = false;
            } catch (IOException e) {
                exception = e;
            }
        }
        if (downloadRequired) {
            throw exception;
        }
        return len;

    }*/

    public String getActualRootPath() {
        return actualRootPath;
    }

    public File getTmpDir() {
        return tmpDir;
    }

    public String checkAndGetPreDownloadedPath(String actualFilePath) {
        return downloadMap.get(actualFilePath);
    }

    public void close() {
        downloadMap.values().stream().forEach(filePath -> new File(filePath).delete());
        new File(downloadFolderPath).delete();
        System.gc();
    }
}
