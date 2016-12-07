package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.NodeUtil;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.BucketCombination;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.sharding.util.ShardingTableCopier;
import com.talentica.hungryHippos.storage.DataStore;
import com.talentica.hungryHippos.storage.FileDataStore;
import com.talentica.hungryHippos.storage.NodeDataStoreIdCalculator;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import com.talentica.hungryhippos.filesystem.util.FileSystemUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by rajkishoreh on 21/11/16.
 */
public class NewDataHandler extends ChannelHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NewDataHandler.class);
    private final NodeDataStoreIdCalculator nodeDataStoreIdCalculator;

    private ByteBuf byteBuf;
    private byte[] previousUnprocessedData;
    private byte[] record;
    private String[] shardingDimensions;
    private ByteBuffer recordBuffer;
    private HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> bucketToNodeNumberMap;
    private FieldTypeArrayDataDescription dataDescription;
    private DataStore dataStore;
    private int recordSize;
    private String storeId;
    private Map<BucketCombination, Set<Node>> bucketCombinationNodeMap;
    private Map<Integer, Set<String>> nodeToFileMap;
    public String DATA_FILE_BASE_NAME = FileSystemContext.getDataFilePrefix();
    private String hhFilePath;
    private int fileId;
    private boolean noError;
    private String uniqueFolderName;


    public NewDataHandler(int fileId, byte[] remainingBufferData) throws HungryHippoException, IOException, InterruptedException, ClassNotFoundException, KeeperException, JAXBException {
        this.noError = true;
        this.fileId = fileId;
        this.previousUnprocessedData = remainingBufferData;
        String path = CoordinationConfigUtil.getZkCoordinationConfigCache()
                .getZookeeperDefaultConfig().getFileidHhfsMapPath();
        hhFilePath = HungryHippoCurator.getInstance()
                .getZnodeData(path + HungryHippoCurator.ZK_PATH_SEPERATOR + fileId + "");
        String dataAbsolutePath = FileSystemContext.getRootDirectory() + hhFilePath;
        String shardingTableFolderPath =
                dataAbsolutePath + File.separatorChar + ShardingTableCopier.SHARDING_ZIP_FILE_NAME;
        updateFilesIfRequired(shardingTableFolderPath);
        ShardingApplicationContext context = new ShardingApplicationContext(shardingTableFolderPath);
        String bucketCombinationPath =
                context.getBucketCombinationtoNodeNumbersMapFilePath();
        bucketCombinationNodeMap =
                ShardingFileUtil.readFromFileBucketCombinationToNodeNumber(bucketCombinationPath);
        dataDescription = context.getConfiguredDataDescription();
        dataDescription.setKeyOrder(context.getShardingDimensions());
        NodeUtil nodeUtil = new NodeUtil(hhFilePath);
        List<String> fileNames = new ArrayList<>();
        bucketToNodeNumberMap = nodeUtil.getBucketToNodeNumberMap();
        shardingDimensions = context.getShardingDimensions();
        nodeToFileMap = new HashMap<>();
        addFileNameToList(fileNames, "", 0,null);
        recordSize = dataDescription.getSize();
        record = new byte[recordSize];
        recordBuffer = ByteBuffer.wrap(record);
        uniqueFolderName = UUID.randomUUID().toString();
        dataStore = new FileDataStore(fileNames, nodeUtil.getKeyToValueToBucketMap().size(),
                dataDescription, hhFilePath, NodeInfo.INSTANCE.getId(), context, uniqueFolderName);

        nodeDataStoreIdCalculator = new NodeDataStoreIdCalculator(nodeUtil.getKeyToValueToBucketMap(),
                nodeUtil.getBucketToNodeNumberMap(), NodeInfo.INSTANCE.getIdentifier(), dataDescription, context);
    }

    private void addFileNameToList(List<String> fileNames, String fileName, int dimension, Map<String, Bucket<KeyValueFrequency>> keyBucket) {
        if (dimension == shardingDimensions.length) {
            addFileName(fileNames, fileName, keyBucket);
            return;
        }
        String key = shardingDimensions[dimension];
        Map<Bucket<KeyValueFrequency>, Node> bucketNodeMap = bucketToNodeNumberMap.get(key);
        for (Map.Entry<Bucket<KeyValueFrequency>, Node> bucketNodeEntry : bucketNodeMap.entrySet()) {
            if (dimension != 0) {
                keyBucket.put(key, bucketNodeEntry.getKey());
                addFileNameToList(fileNames, new String(fileName + "_" +bucketNodeEntry.getKey().getId()), dimension + 1, keyBucket);
            } else if (bucketNodeEntry.getValue().getNodeId() == NodeInfo.INSTANCE.getIdentifier()) {
                keyBucket = new HashMap<>();
                keyBucket.put(key, bucketNodeEntry.getKey());
                addFileNameToList(fileNames, new String(bucketNodeEntry.getKey().getId()+fileName), dimension + 1, keyBucket);
            }

        }
    }

    private void addFileName(List<String> fileNames, String fileName, Map<String, Bucket<KeyValueFrequency>> keyBucket) {
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
        fileNames.add(fileName);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        LOGGER.info("Inside handlerAdded");
        String senderIp = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
        LOGGER.info("Connected to {}", senderIp);
        this.byteBuf = ctx.alloc().buffer(recordSize * 20);
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
        while (byteBuf.readableBytes() >= recordSize) {
            byteBuf.readBytes(record);
            storeId = nodeDataStoreIdCalculator.storeId(recordBuffer);
            dataStore.storeRow(storeId, record);
        }
    }

    private void reInitializeByteBuf() {
        int remainingBytes = byteBuf.readableBytes();
        if (remainingBytes > 0) {
            byteBuf.readBytes(record, 0, remainingBytes);
            byteBuf.clear();
            byteBuf.writeBytes(record, 0, remainingBytes);
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

        FileJoiner.join(srcFolderPath,destFolderPath);
        byteBuf = null;
        try {
            if (!checkIfFailed(hhFilePath)&&noError) {
                HHFileUploader.uploadFile(destFolderPath,hhFilePath,nodeToFileMap);
                clearNode();
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.toString());
            updateFailure(hhFilePath,NodeInfo.INSTANCE.getIp()+" : "+e.toString());
        }
        ctx.channel().close();
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
        curator.createPersistentNodeIfNotPresent(pathForFailureNode, cause);
    }

    public static boolean checkIfFailed(String hhFilePath) throws HungryHippoException {
        HungryHippoCurator curator = HungryHippoCurator.getInstance();
        String destinationPathNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
                .getZookeeperDefaultConfig().getFilesystemPath() + hhFilePath;
        String pathForFailureNode = destinationPathNode + HungryHippoCurator.ZK_PATH_SEPERATOR
                + FileSystemConstants.PUBLISH_FAILED;
        return  curator.checkExists(pathForFailureNode);
    }

    public void clearNode() {
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
        updateFailure(hhFilePath,NodeInfo.INSTANCE.getIp()+" : "+cause.toString());
        cause.printStackTrace();
        LOGGER.error(cause.toString());
        ctx.channel().close();

    }
}
