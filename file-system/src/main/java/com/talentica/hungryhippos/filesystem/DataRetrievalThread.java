package com.talentica.hungryhippos.filesystem;

import java.io.*;
import java.net.Socket;

/**
 * This class is for creating threads for handling each client request for Data Retrieval
 * Created by rajkishoreh on 30/6/16.
 */
public class DataRetrievalThread extends Thread {

    FileInputStream fis = null;
    BufferedInputStream bis = null;
    Socket clientSocket = null;
    DataInputStream dis = null;
    DataOutputStream dos = null;
    private static final int BUFFER_SIZE = 1024;


    DataRetrievalThread(Socket clientSocket) {
        this.clientSocket = clientSocket;
        System.out.println(Thread.currentThread().getName() + " Just connected to " + clientSocket.getRemoteSocketAddress());
    }

    public void run() {

        try {

            dis = new DataInputStream(clientSocket.getInputStream());
            dos = new DataOutputStream(clientSocket.getOutputStream());

            //Tells client that the server has assigned a thread for Data retrieval
            dos.writeUTF(FileSystemConstants.DATA_SERVER_AVAILABLE);

            //Reads the request for filePaths
            String filePaths = dis.readUTF();
            System.out.println(Thread.currentThread().getName() + " FilePaths :" + filePaths);
            String[] filePathsArr = filePaths.split(FileSystemConstants.FILE_PATHS_SEPARATOR);

            //Determines the aggregated size of the files
            long totalFilesSize = 0;
            for (String filePath : filePathsArr) {
                totalFilesSize += new File(filePath).length();
            }

            //Informs the client that about the aggregated file size
            dos.writeLong(totalFilesSize);

            byte[] inputBuffer = new byte[BUFFER_SIZE];
            int len;

            //Sends files data back to back on the same stream
            for (String filePath : filePathsArr) {

                System.out.println(Thread.currentThread().getName() + " Sending Data of :" + filePath);

                fis = new FileInputStream(filePath);
                bis = new BufferedInputStream(fis);

                while ((len = bis.read(inputBuffer)) > -1) {
                    dos.write(inputBuffer, 0, len);
                }
                fis.close();
                dos.flush();
                System.out.println(Thread.currentThread().getName() + " dos.size() :" + dos.size());
            }
            Thread.sleep(1000);

            //informs that the file data has been transferred completely
            dos.writeUTF(FileSystemConstants.DATA_TRANSFER_COMPLETED);
            System.out.println(Thread.currentThread().getName() + " " + FileSystemConstants.DATA_TRANSFER_COMPLETED);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
                if (dis != null) {
                    dis.close();
                }
                if (dos != null) {
                    dos.close();
                }
                if (fis != null) {
                    fis.close();
                }
                if (bis != null) {
                    bis.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


        }

    }
}
