package com.talentica.hungryHippos.node.joiners;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.node.DataDistributorStarter;
import com.talentica.hungryHippos.node.service.CacheClearService;
import com.talentica.hungryHippos.node.service.DataDistributor;
import com.talentica.hungryHippos.sharding.Bucket;
import com.talentica.hungryHippos.sharding.KeyValueFrequency;
import com.talentica.hungryHippos.sharding.Node;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.sharding.util.ShardingFileUtil;
import com.talentica.hungryHippos.storage.ResourceAllocator;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 17/5/17.
 */
public class NodeFileJoiner implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeFileJoiner.class);

    private volatile Queue<String> fileSrcQueue;

    private String hhFilePath;

    public NodeFileJoiner(Queue<String> fileSrcQueue, String hhFilePath) {
        this.fileSrcQueue = fileSrcQueue;
        this.hhFilePath = hhFilePath;
    }

    @Override
    public Boolean call() throws Exception {
        if (fileSrcQueue.isEmpty()) {
            return true;
        }
        String shardingTablePath = DataDistributor.getShardingTableLocation(hhFilePath);
        ShardingApplicationContext context = new ShardingApplicationContext(shardingTablePath);
        FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
        dataDescription.setKeyOrder(context.getShardingDimensions());
        String[] keyOrder = context.getShardingDimensions();
        int offset = 4;
        int dataSize = dataDescription.getSize();
        String keyToBucketToNodePath = context.getBuckettoNodeNumberMapFilePath();

        HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> keyToBucketToNodetMap =
                ShardingFileUtil.readFromFileBucketToNodeNumber(keyToBucketToNodePath);
        NodeFileMapper nodeFileMapper = new NodeFileMapper(hhFilePath, context,
                keyToBucketToNodetMap, keyOrder);
        int index;
        String srcFileName;
        while ((srcFileName = fileSrcQueue.poll()) != null) {
            System.gc();
            byte[] buf = new byte[offset + dataSize];
            ByteBuffer indexBuffer = ByteBuffer.wrap(buf);
            File srcFile = new File(srcFileName);
            File srcFolder = new File(srcFile.getParent());
            if(!nodeFileMapper.isUsingBufferStream()){
                if(ResourceAllocator.INSTANCE.isMemoryAvailableForBuffer(nodeFileMapper.getNoOfFiles())){
                    LOGGER.info("Upgrading store for {}", srcFileName);
                    nodeFileMapper.upgradeStore();
                }
            }
            try {
                LOGGER.info("Processing file {}", srcFileName);
                if (srcFile.exists()) {
                    FileInputStream fis = new FileInputStream(srcFile);
                    BufferedInputStream bis = new BufferedInputStream(fis);
                    int len;
                    while ((len = bis.read(buf)) != -1) {
                        index = indexBuffer.getInt(0);
                        nodeFileMapper.storeRow(index, buf, offset, dataSize);
                    }
                    bis.close();
                    srcFile.delete();
                    FileUtils.deleteDirectory(srcFolder);
                    DataDistributorStarter.cacheClearServices.execute(new CacheClearService());
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }

        }
        nodeFileMapper.sync();
        return true;
    }
}
