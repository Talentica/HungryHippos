package com.talentica.hungryHippos.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.context.DataPublisherApplicationContext;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;

/**
 * Created by rajkishoreh on 8/9/16.
 */
public class ReplicaDataSender extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaDataSender.class);
  private Socket socket;
  private BufferedOutputStream bos;
  private String destinationPath;
  private boolean keepAlive = true;
  private byte[][] memoryBlocks;

  public enum Status {
    SENDING_BLOCK, ENABLE_BLOCK_WRITE, ENABLE_BLOCK_READ
  }

  private Status[] memoryBlockStatus;

  public ReplicaDataSender(String nodeIp, int port, String destinationPath, byte[][] memoryBlocks)
      throws IOException {
    this.destinationPath = destinationPath;
    this.memoryBlocks = memoryBlocks;
    this.memoryBlockStatus = new Status[memoryBlocks.length];
    for (int i = 0; i < memoryBlocks.length; i++) {
      this.memoryBlockStatus[i] = Status.ENABLE_BLOCK_WRITE;
    }
    establishConnection(nodeIp, port);
  }

  /**
   * @throws IOException
   */
  private void establishConnection(String nodeIp, int port) throws IOException {
    LOGGER.info("Establishing Connections");

    LOGGER.info("Connecting to {}", nodeIp);
    byte[] destinationPathInBytes = destinationPath.getBytes(Charset.defaultCharset());
    int destinationPathLength = destinationPathInBytes.length;
    int noOfAttemptsToConnectToNode = DataPublisherApplicationContext.getNoOfAttemptsToConnectToNode();
    while (true) {
      try {
        socket = new Socket(nodeIp, port);
        break;
      } catch (IOException e) {
        if (noOfAttemptsToConnectToNode == 0) {
          throw new RuntimeException("Couldn't connect to server " + nodeIp);
        }
        LOGGER.error(e.toString());
        LOGGER.info("Retrying connection after 1 second");
        try {
          Thread.sleep(DataPublisherApplicationContext.getServersConnectRetryIntervalInMs());
        } catch (InterruptedException e1) {
          LOGGER.error(e1.toString());
        }
        noOfAttemptsToConnectToNode--;
      }
    }
    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
    bos = new BufferedOutputStream(socket.getOutputStream(), 8388608);
    dos.writeInt(destinationPathLength);
    dos.flush();
    bos.write(destinationPathInBytes);
    bos.write((byte) -1);
    bos.flush();
    LOGGER.info("Connected to {}", nodeIp);
    LOGGER.info("Established Connections");
  }


  @Override
  public void run() {
    LOGGER.info("Started publishing replica data");
    while (keepAlive) {
      try {
        publishReplicaData();
      } catch (IOException e) {
        LOGGER.error(e.toString());
        throw new RuntimeException(e);
      }
    }
    try {
      publishReplicaData();
    } catch (IOException e) {
      LOGGER.error(e.toString());
    }
    LOGGER.info("Completed publishing replica data");
  }

  /**
   * Publishes the replica data to the all the other nodes
   *
   * @throws IOException
   */
  private void publishReplicaData() throws IOException {
    for (int i = 0; i < memoryBlocks.length; i++) {
      if (memoryBlockStatus[i] == Status.ENABLE_BLOCK_READ) {
        memoryBlockStatus[i] = Status.SENDING_BLOCK;
        bos.write(memoryBlocks[i]);
        bos.flush();
        memoryBlockStatus[i] = Status.ENABLE_BLOCK_WRITE;
      }
    }
  }

  public void publishRemainingReplicaData(int length, int blockIdx) throws IOException {
    bos.write(memoryBlocks[blockIdx], 0, length);
    bos.flush();
  }


  public void kill() {
    keepAlive = false;
  }

  public Status getMemoryBlockStatus(int blockIdx) {
    return memoryBlockStatus[blockIdx];
  }

  public void setMemoryBlockStatus(Status memoryBlock1Status, int blockIdx) {
    this.memoryBlockStatus[blockIdx] = memoryBlock1Status;
  }

  public void closeConnection() {
    try {
      if (bos != null) {
        bos.flush();
        bos.close();
      }
      if (socket != null) {
        socket.close();
      }
    } catch (IOException e) {
      LOGGER.error(e.toString());
    }
    LOGGER.info("Closed connections");
  }


  public void clearReferences() {
    memoryBlocks = null;
    memoryBlockStatus = null;
  }
}
