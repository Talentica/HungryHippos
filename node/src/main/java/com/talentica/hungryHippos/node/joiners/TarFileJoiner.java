package com.talentica.hungryHippos.node.joiners;

import com.talentica.hungryHippos.utility.scp.TarAndUntar;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Created by rajkishoreh on 19/5/17.
 */
public class TarFileJoiner implements Callable<Boolean>{

    private static final Logger LOGGER = LoggerFactory.getLogger(TarFileJoiner.class);

    private volatile Queue<String> fileSrcQueue;

    private String hhFilePath;

    public TarFileJoiner(Queue<String> fileSrcQueue, String hhFilePath) {
        this.fileSrcQueue = fileSrcQueue;
        this.hhFilePath = hhFilePath;
    }

    @Override
    public Boolean call() throws Exception {
        if (fileSrcQueue.isEmpty()) {
            return true;
        }
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
        return true;
    }
}
