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
import com.talentica.hungryHippos.utility.scp.TarAndUntar;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 19/5/17.
 */
public class TarFileJoiner implements Callable<Boolean>{

    private static final Logger LOGGER = LoggerFactory.getLogger(TarFileJoiner.class);

    private volatile Queue<String> fileSrcQueue;

    private String hhFilePath;

    private UnTarStrategy unTarStrategy;

    public TarFileJoiner(Queue<String> fileSrcQueue, String hhFilePath,UnTarStrategy unTarStrategy) {
        this.fileSrcQueue = fileSrcQueue;
        this.hhFilePath = hhFilePath;
        this.unTarStrategy = unTarStrategy;
    }

    @Override
    public Boolean call() throws Exception {
        if (fileSrcQueue.isEmpty()) {
            return true;
        }
        if(unTarStrategy==UnTarStrategy.UNTAR_ON_CONTINUOUS_STREAMS){
            untarOnContinuousStreams();
        }
        else {
            untarOneFileAtATime();
        }
        return true;
    }

    private void untarOneFileAtATime() throws IOException {
        String destFolderPath = FileSystemContext.getRootDirectory() + hhFilePath + File.separator + FileSystemContext.getDataFilePrefix();
        String srcFileName;
        while ((srcFileName = fileSrcQueue.poll()) != null) {
            System.gc();
            File srcTarFile = new File(srcFileName);
            File srcFolder = srcTarFile.getParentFile();
            LOGGER.info("Processing file {}", srcFileName);
            try {
                TarAndUntar.untarAndAppend(srcFileName, destFolderPath);
            } catch (FileNotFoundException e) {
                LOGGER.error("[{}] Retrying File untar for {}", Thread.currentThread().getName(), srcFileName);
            }
            srcTarFile.delete();
            FileUtils.deleteDirectory(srcFolder);
        }
    }

    private void untarOnContinuousStreams() throws JAXBException, InterruptedException, ClassNotFoundException, KeeperException, IOException {
        String shardingTablePath = DataDistributor.getShardingTableLocation(hhFilePath);
        ShardingApplicationContext context = new ShardingApplicationContext(shardingTablePath);
        FieldTypeArrayDataDescription dataDescription = context.getConfiguredDataDescription();
        dataDescription.setKeyOrder(context.getShardingDimensions());
        String[] keyOrder = context.getShardingDimensions();
        String keyToBucketToNodePath = context.getBuckettoNodeNumberMapFilePath();

        HashMap<String, HashMap<Bucket<KeyValueFrequency>, Node>> keyToBucketToNodetMap =
                ShardingFileUtil.readFromFileBucketToNodeNumber(keyToBucketToNodePath);
        NodeFileMapper nodeFileMapper = new NodeFileMapper(hhFilePath, context,
                keyToBucketToNodetMap, keyOrder);
        String srcFileName;
        while ((srcFileName = fileSrcQueue.poll()) != null) {
            System.gc();
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
                    if(nodeFileMapper.isUsingBufferStream()){
                        TarAndUntar.untarToStream(srcFileName,nodeFileMapper.getFileNameToBufferedOutputStreamMap());
                    }else{
                        TarAndUntar.untarToStream(srcFileName,nodeFileMapper.getFileNameToOutputStreamMap());
                    }
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
    }
}
