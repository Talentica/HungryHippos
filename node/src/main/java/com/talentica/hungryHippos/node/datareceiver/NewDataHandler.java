package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Created by rajkishoreh on 21/11/16.
 */
public class NewDataHandler extends ChannelHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NewDataHandler.class);

    private ByteBuf byteBuf;
    private byte[] previousUnprocessedData;
    private byte[] record;
    private byte[] bufferForReInit;
    private String[] shardingDimensions;
    private HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;
    private FieldTypeArrayDataDescription dataDescription;
    private DataStore dataStore;
    private int recordSize;
    private Map<BucketCombination, Set<Node>> bucketCombinationNodeMap;
    private Map<Integer, Set<String>> nodeToFileMap;
    public String DATA_FILE_BASE_NAME = FileSystemContext.getDataFilePrefix();
    private String hhFilePath;
    private int fileId;
    private boolean noError;
    private String uniqueFolderName;
    private String senderIp;
    private int maxBucketSize;
    private static int SIZE_OF_INT = 4;
    private int dataFileIndex;
    private int blockSize;


    public NewDataHandler(int fileId, byte[] remainingBufferData) throws HungryHippoException, IOException, InterruptedException, ClassNotFoundException, KeeperException, JAXBException {
        this.noError = true;
        this.fileId = fileId;
        this.previousUnprocessedData = remainingBufferData;
        HungryHippoCurator curator = HungryHippoCurator.getInstance();
        String fileIdNodePath = getHHFileIdPath(fileId);
        int noOfAttemptsLeft= 25;
        while(noOfAttemptsLeft>0&&!curator.checkExists(fileIdNodePath)){
            LOGGER.info("{} does not exists. Retrying after 2 seconds",fileIdNodePath);
            Thread.sleep(2000);
            noOfAttemptsLeft--;
        }
        if(!curator.checkExists(fileIdNodePath)){
            throw new RuntimeException(fileIdNodePath +" in zookeper not found");
        }
        hhFilePath = curator.getZnodeData(fileIdNodePath);
        String dataAbsolutePath = FileSystemContext.getRootDirectory() + hhFilePath;
        String shardingTableFolderPath =
                dataAbsolutePath + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
        updateFilesIfRequired(shardingTableFolderPath);
        ShardingApplicationContext context = new ShardingApplicationContext(shardingTableFolderPath);
        String bucketCombinationPath =
                context.getBucketCombinationtoNodeNumbersMapFilePath();
        bucketCombinationNodeMap =
                ShardingFileUtil.readFromFileBucketCombinationToNodeNumber(bucketCombinationPath);
        String keyToBucketToNodePath =
                context.getBuckettoNodeNumberMapFilePath();
        shardingDimensions = context.getShardingDimensions();
        dataDescription = context.getConfiguredDataDescription();
        dataDescription.setKeyOrder(shardingDimensions);
        Map<Integer,String> fileNames = new HashMap<>();
        bucketToNodeNumberMap = ShardingFileUtil.readFromFileBucketToNodeNumber(keyToBucketToNodePath);

        nodeToFileMap = new HashMap<>();
        maxBucketSize = Integer.parseInt(context.getShardingServerConfig().getMaximumNoOfShardBucketsSize());
        addFileNameToList(fileNames,0, "", 0,null);
        recordSize = dataDescription.getSize();
        record = new byte[recordSize];
        blockSize = recordSize+SIZE_OF_INT;
        bufferForReInit = new byte[blockSize];
        uniqueFolderName = UUID.randomUUID().toString();
        dataStore = new FileDataStore(fileNames, maxBucketSize,shardingDimensions.length,
                dataDescription, hhFilePath, NodeInfo.INSTANCE.getId(), context, uniqueFolderName);
    }

    private void addFileNameToList(Map<Integer,String> fileNames,int index,String fileName, int dimension, Map<String, Bucket<KeyValueFrequency>> keyBucket) {
        if (dimension == shardingDimensions.length) {
            addFileName(fileNames,index, fileName, keyBucket);
            return;
        }
        String key = shardingDimensions[dimension];
        Map<Bucket<KeyValueFrequency>, Node> bucketNodeMap = bucketToNodeNumberMap.get(key);
        for (Map.Entry<Bucket<KeyValueFrequency>, Node> bucketNodeEntry : bucketNodeMap.entrySet()) {

            if (dimension != 0) {
                keyBucket.put(key, bucketNodeEntry.getKey());
                int bucketId = bucketNodeEntry.getKey().getId();
                int newIndex = index + bucketId*(int)Math.pow(maxBucketSize,dimension);
                addFileNameToList(fileNames,newIndex, new String(fileName + "_" +bucketId), dimension + 1, keyBucket);
            } else if (bucketNodeEntry.getValue().getNodeId() == NodeInfo.INSTANCE.getIdentifier()) {
                keyBucket = new HashMap<>();
                keyBucket.put(key, bucketNodeEntry.getKey());
                int bucketId = bucketNodeEntry.getKey().getId();
                int newIndex = index + bucketId*(int)Math.pow(maxBucketSize,dimension);
                addFileNameToList(fileNames,newIndex, new String(bucketId+fileName), dimension + 1, keyBucket);
            }

        }
    }

    private void addFileName(Map<Integer,String> fileNames,int index, String fileName, Map<String, Bucket<KeyValueFrequency>> keyBucket) {
        BucketCombination bucketCombination = new BucketCombination(keyBucket);
        Set<Node> nodes = bucketCombinationNodeMap.get(bucketCombination);
        Iterator<Node> nodeIterator = nodes.iterator();
        nodeIterator.next();
        while (nodeIterator.hasNext()) {
            Node node = nodeIterator.next();
            int nodeId = node.getNodeId();
            Set<String> fileNameSet = nodeToFileMap.get(nodeId);
            if (fileNameSet == null) {
                fileNameSet = new HashSet<>();
                nodeToFileMap.put(nodeId, fileNameSet);
            }
            fileNameSet.add(fileName);
        }
        fileNames.put(index,fileName);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        LOGGER.info("Inside handlerAdded");
        senderIp = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
        LOGGER.info("Connected to {} FileId : {}", senderIp,fileId);
        this.byteBuf = ctx.alloc().buffer(blockSize * 200);
        this.byteBuf.writeBytes(previousUnprocessedData);
        processData();
        LOGGER.info("Exiting handlerAdded");
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgB = (ByteBuf) msg;
        byteBuf.writeBytes(msgB);
        msgB.release();
        processData();
    }

    private void processData() {
        writeDataInStore();
        reInitializeByteBuf();
    }

    private void writeDataInStore() {
        while (byteBuf.readableBytes() >= blockSize) {
            dataFileIndex = byteBuf.readInt();
            byteBuf.readBytes(record);
            dataStore.storeRow(dataFileIndex, record);
        }
    }

    private void reInitializeByteBuf() {
        int remainingBytes = byteBuf.readableBytes();
        if (remainingBytes > 0) {
            byteBuf.readBytes(bufferForReInit, 0, remainingBytes);
            byteBuf.clear();
            byteBuf.writeBytes(bufferForReInit, 0, remainingBytes);
        } else {
            byteBuf.clear();
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws InterruptedException, IOException {
        LOGGER.info("Inside handlerRemoved");
        writeDataInStore();
        byteBuf.release();
        dataStore.sync();
        String baseFolderPath =  FileSystemContext.getRootDirectory() + hhFilePath;
        String srcFolderPath = baseFolderPath + File.separator + uniqueFolderName;
        String destFolderPath = baseFolderPath + File.separator + DATA_FILE_BASE_NAME;
        String metadataFilePath = baseFolderPath + File.separator + FileSystemConstants.META_DATA_FOLDER_NAME
                +File.separator+NodeInfo.INSTANCE.getId();
        byteBuf = null;
        try {
            FileJoiner.INSTANCE.join(srcFolderPath,destFolderPath,destFolderPath,metadataFilePath,hhFilePath);
            HHFileUploader.INSTANCE.uploadFile(srcFolderPath,destFolderPath,nodeToFileMap, hhFilePath);
            FileUtils.deleteDirectory(new File(srcFolderPath));
            if (!checkIfFailed(hhFilePath)&&noError) {
                clearNode(fileId);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.toString());
            updateFailure(hhFilePath,e.toString());
        }
        LOGGER.info("Closed connection from {} FileId : {}",senderIp,fileId);
        ctx.channel().close();
        System.gc();
        LOGGER.info("Exiting handlerRemoved");
    }

    /**
     * Updates the sharding files if required
     *
     * @param shardingTableFolderPath
     * @throws IOException
     */
    public static void updateFilesIfRequired(String shardingTableFolderPath) throws IOException {
        String shardingTableZipPath = shardingTableFolderPath + ".tar.gz";
        File shardingTableFolder = new File(shardingTableFolderPath);
        File shardingTableZip = new File(shardingTableZipPath);
        if (shardingTableFolder.exists()) {
            if (shardingTableFolder.lastModified() < shardingTableZip.lastModified()) {
                FileSystemUtils.deleteFilesRecursively(shardingTableFolder);
                TarAndGzip.untarTGzFile(shardingTableZipPath);
            }
        } else {
            TarAndGzip.untarTGzFile(shardingTableZipPath);
        }
    }

    public static void updateFailure(String hhFilePath, String cause) {
        HungryHippoCurator curator = HungryHippoCurator.getInstance();
        String destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
                .getZookeeperDefaultConfig().getFilesystemPath() + hhFilePath;
        String pathForFailureNode = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
                + FileSystemConstants.PUBLISH_FAILED;
        try {
            if(checkIfFailed(hhFilePath)){
                cause =  cause + "\n" + curator.getZnodeData(pathForFailureNode);
            }
        } catch (HungryHippoException e) {
            e.printStackTrace();
        }
        curator.createPersistentNodeIfNotPresent(pathForFailureNode, NodeInfo.INSTANCE.getIp()+" : "+ cause);
    }

    public static boolean checkIfFailed(String hhFilePath) throws HungryHippoException {
        HungryHippoCurator curator = HungryHippoCurator.getInstance();
        String destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
                .getZookeeperDefaultConfig().getFilesystemPath() + hhFilePath;
        String pathForFailureNode = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
                + FileSystemConstants.PUBLISH_FAILED;
        return  curator.checkExists(pathForFailureNode);
    }

    public static void clearNode(int fileId) {
        String hhFileIdNodePath = getHHFileIdNodePath(fileId);
        HungryHippoCurator curator = HungryHippoCurator.getInstance();
        try {
            curator.deletePersistentNodeIfExits(hhFileIdNodePath);
        } catch (HungryHippoException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }


    public static String getHHFileIdNodePath(int fileId) {
        String hhFileIdPath = getHHFileIdPath(fileId);
        return hhFileIdPath + HungryHippoCurator.ZK_PATH_SEPERATOR + NodeInfo.INSTANCE.getIdentifier();
    }

    public static String getHHFileIdPath(int fileId) {
        String hhFileIdRootPath = CoordinationConfigUtil.getZkCoordinationConfigCache()
                .getZookeeperDefaultConfig().getFileidHhfsMapPath();
        return hhFileIdRootPath + HungryHippoCurator.ZK_PATH_SEPERATOR + fileId;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        this.noError = false;
        updateFailure(hhFilePath,cause.toString());
        cause.printStackTrace();
        LOGGER.error(cause.toString());
        ctx.channel().close();

    }
}
