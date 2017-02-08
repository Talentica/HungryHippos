package com.talentica.hungryHippos.master;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.Chunk;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;

/**
 * Created by rajkishoreh on 19/12/16.
 */
public class ChunkUpload implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(ChunkUpload.class);
  Map<Integer, DataInputStream> dataInputStreamMap;
  Map<Integer, Socket> socketMap;
  boolean success;
  private String destinationPath;
  private String remotePath;
  private List<Node> nodes;



  public ChunkUpload(String destinationPath, String remotePath,
      Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap,
      List<Node> nodes) {
    this.destinationPath = destinationPath;
    this.remotePath = remotePath;
    this.dataInputStreamMap = dataInputStreamMap;
    this.socketMap = socketMap;
    this.success = false;
    this.nodes = nodes;
  }

  @Override
  public void run() {

    try {

      logger.info("[{}] Uploading chunk ", Thread.currentThread().getName());
      while (!DataPublisherStarter.queue.isEmpty()) {

        DataPublisherStarter.uploadChunk(destinationPath, nodes, remotePath, dataInputStreamMap,
            socketMap);
      }
      success = true;

      logger.info("[{}] Upload is successful for chunk", Thread.currentThread().getName());
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      logger.error("[{}] File publish failed for {}", Thread.currentThread().getName(),
          destinationPath);
      throw new RuntimeException("File Publish failed");
    }
  }

  public boolean isSuccess() {
    return success;
  }


}
