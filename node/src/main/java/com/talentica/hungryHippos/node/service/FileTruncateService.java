package com.talentica.hungryHippos.node.service;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.node.NodeInfo;
import com.talentica.hungryHippos.node.job.listener.DataPublishFailure;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

public class FileTruncateService implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(FileTruncateService.class);
  String destinationPath = null;
  private Socket socket = null;

  public FileTruncateService(Socket socket) {
    this.socket = socket;
  }

  @Override
  public void run() {

    DataInputStream dis = null;

    try {

      dis = new DataInputStream(socket.getInputStream());
      destinationPath = dis.readUTF();

      logger.info("destination path {}", destinationPath);

      DataPublishFailure.register(destinationPath);

      logger.info("successfully registered listener");

    } catch (IOException e) {

      e.printStackTrace();

    }

  }


}

