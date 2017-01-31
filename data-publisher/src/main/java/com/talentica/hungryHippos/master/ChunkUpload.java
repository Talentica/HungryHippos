package com.talentica.hungryHippos.master;

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
  Chunk chunk;
  List<Node> nodes;
  Map<Integer, DataInputStream> dataInputStreamMap;
  Map<Integer, Socket> socketMap;
  boolean success;
  private String destinationPath;
  private String remotePath;


  public ChunkUpload(Chunk chunk, List<Node> nodes, String destinationPath, String remotePath,
      Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap) {
    this.chunk = chunk;
    this.nodes = nodes;
    this.destinationPath = destinationPath;
    this.remotePath = remotePath;
    this.dataInputStreamMap = dataInputStreamMap;
    this.socketMap = socketMap;
    this.success = false;
  }

  @Override
  public void run() {


    try {
      int nodeId = chunk.getId() % nodes.size();
      
      logger.info("[{}] Uploading chunk {}", Thread.currentThread().getName(), chunk.getId());
      DataPublisherStarter.uploadChunk(destinationPath, nodes, remotePath, dataInputStreamMap,
          socketMap, this.chunk, nodeId);
      this.success = true;
      logger.info("[{}] Upload is successful for chunk {}", Thread.currentThread().getName(),
          chunk.getId());
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      logger.error("[{}] {} File publish failed for {}", Thread.currentThread().getName(),
          chunk.getFileName(), destinationPath);
      throw new RuntimeException("File Publish failed");
    }
  }

  public boolean isSuccess() {
    return success;
  }
}
