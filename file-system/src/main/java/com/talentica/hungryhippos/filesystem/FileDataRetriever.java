package com.talentica.hungryhippos.filesystem;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

import com.talentica.hungryHippos.coordination.NodesManager;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryhippos.config.client.CoordinationServers;
import com.talentica.hungryhippos.config.client.ObjectFactory;

/**
 * Created by rajkishoreh on 29/6/16.
 */
public class FileDataRetriever {

  private static final int MAX_NO_OF_ATTEMPTS = 10;
  private static final int BUFFER_SIZE = 1024;

  /**
   * This method is for reading the metadata from the zookeeper node and retrieving the data on a
   * particular dimension
   *
   * @param fileZKNode
   * @param outputDirName
   * @param noOfAttempts No of attempts made by the method for retrieving the data
   * @param dimension
   * @throws Exception
   */
  public static void retrieveZookeeperData(String fileZKNode, String outputDirName,
      int noOfAttempts, int dimension) throws Exception {

    // checks for invalid dimension and throws exception
    if (dimension < 0) {
      throw new RuntimeException("Invalid Dimension : " + dimension);
    }

    // Determines the dataFileNodes from which the data has to be retrieved.
    int binaryDimensionOperand = 1;
    if (dimension > 0) {
      binaryDimensionOperand = binaryDimensionOperand << (dimension - 1);
    } else {
      // if dimension specified is zero , then all the dataFileNodes are selected.
      binaryDimensionOperand = 0;
    }

    // Creates local directory structure if not present
    File outputDirectory = new File(outputDirName);
    if (!outputDirectory.exists()) {
      outputDirectory.mkdirs();
    }

    // Gets the NodeIps having data of the fileZKNode
    CoordinationServers coordinationServers = new ObjectFactory().createCoordinationServers();
    NodesManager nodesManager = NodesManagerContext.getNodesManagerInstance();
    List<String> nodeIps = nodesManager.getChildren(fileZKNode);

    // iterates on NodeIps
    int count = 0;
    for (String nodeIp : nodeIps) {

      // Gets the datafileNodes in nodeIp having data of the fileZKNode
      List<String> datafileNodes = nodesManager.getChildren(fileZKNode + File.separator + nodeIp);


      // iterates on each node to create a string containing the dataFile Paths
      StringBuilder dataFilePaths = new StringBuilder();
      int i = 0;
      for (String dataFileNode : datafileNodes) {
        // Determines the file associated with dataFileNode
        int dataFileNodeInt = Integer.parseInt(dataFileNode);
        if ((dataFileNodeInt & binaryDimensionOperand) == binaryDimensionOperand) {
          System.out.println("Data_" + dataFileNodeInt);
          String nodeDataString =
              (String) nodesManager.getObjectFromZKNode(fileZKNode + File.separator + nodeIp
                  + File.separator + dataFileNode);
          String[] nodeDataArr = nodeDataString.split(",");
          i++;
          if (i != datafileNodes.size()) {
            dataFilePaths.append(nodeDataArr[0]).append(",");
          } else {
            dataFilePaths.append(nodeDataArr[0]);
          }

        }


      }
      System.out.println(dataFilePaths.toString());

      requestDataFile(nodeIp, dataFilePaths.toString(), outputDirName + File.separator + "part-"
          + count++, noOfAttempts);
    }
  }


  /**
   * This method is used for requesting the DataRetrievalRequestListener for the files
   *
   * @param nodeIp
   * @param dataFilePaths
   * @param outputFile
   * @param noOfAttemptsLeft
   */
  public static void requestDataFile(String nodeIp, String dataFilePaths, String outputFile,
      int noOfAttemptsLeft) {
    noOfAttemptsLeft--;
    System.out.println("Number of tries left" + noOfAttemptsLeft);
    if (noOfAttemptsLeft == 0) {
      throw new RuntimeException("Data retrieval failed");
    }


    try (Socket client = new Socket(nodeIp, 9898);
        DataOutputStream dos = new DataOutputStream(client.getOutputStream());
        DataInputStream dis = new DataInputStream(client.getInputStream());
        FileOutputStream fos = new FileOutputStream(outputFile);
        BufferedOutputStream bos = new BufferedOutputStream(fos);) {
      // Gets response from the server whether the server has any slot for serving the request
      String processStatus = dis.readUTF();
      if (FileSystemConstants.DATA_SERVER_AVAILABLE.equals(processStatus)) {
        System.out.println(processStatus);
        System.out.println("Recieving data into " + outputFile);

        // Send the files for transfer
        dos.writeUTF(dataFilePaths);

        int len;
        byte[] inputBuffer = new byte[BUFFER_SIZE];

        // Reads the file size
        long fileSize = dis.readLong();
        while (fileSize > 0 && (len = dis.read(inputBuffer)) > -1) {
          bos.write(inputBuffer, 0, len);
          fileSize = fileSize - len;
        }
        bos.flush();
        System.out.println(dis.readUTF());

      } else {
        // Waits for 10000 milliseconds and retries for data retrieval
        System.out.println(processStatus);
        System.out.println("Retrying after 10000 milliseconds");
        Thread.sleep(10000);
        requestDataFile(nodeIp, dataFilePaths, outputFile, noOfAttemptsLeft);

      }


    } catch (IOException e1) {
      e1.printStackTrace();
      requestDataFile(nodeIp, dataFilePaths, outputFile, noOfAttemptsLeft);
    } catch (InterruptedException e2) {
      e2.printStackTrace();
      requestDataFile(nodeIp, dataFilePaths, outputFile, noOfAttemptsLeft);
    }

  }

  public static void main(String[] args) throws Exception {
    int dimension = 1;
    retrieveZookeeperData("/filesystem/input", "/home/rajkishoreh/hungryhips/inp_"
        + Thread.currentThread().getId(), MAX_NO_OF_ATTEMPTS, dimension);


    /*
     * for (int i = 0; i < 25; i++) { new Thread(new Runnable() {
     * 
     * @Override public void run() { try { retrieveZookeeperData("/filesystem/input",
     * "/home/rajkishoreh/hungryhips/inp_" + Thread.currentThread().getId(), MAX_NO_OF_ATTEMPTS); }
     * catch (Exception e) { e.printStackTrace(); } } }).start();
     * 
     * }
     */

  }

}
