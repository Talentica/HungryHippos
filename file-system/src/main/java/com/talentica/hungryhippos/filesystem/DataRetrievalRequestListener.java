package com.talentica.hungryhippos.filesystem;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This class is for creating a Server which listens at a particular port
 * and creates a Thread for handling client requests
 * Created by rajkishoreh on 30/6/16.
 */
public class DataRetrievalRequestListener {

    public static final int MAX_CLIENT_REQUESTS = 20;
    private static final DataRetrievalThread[] REQUEST_HANDLING_THREADS = new DataRetrievalThread[MAX_CLIENT_REQUESTS];


    /**
     * This method is for creating a server which handles clients request for Data Retrieval
     *
     * @throws IOException
     */
    public void listen() throws IOException {

        ServerSocket serverSocket = new ServerSocket(9898);

        // Runs indefinitely till the process is killed
        while (true) {
            try {
                //waits till a client request is accepted
                System.out.println(Thread.currentThread().getName() + " Waiting for client on port " + serverSocket.getLocalPort() + "...");
                Socket clientSocket = serverSocket.accept();
                int i = 0;
                for (i = 0; i < MAX_CLIENT_REQUESTS; i++) {

                    //Checks if there REQUEST_HANDLING_THREADS contains empty slots or if the threads in the REQUEST_HANDLING_THREADS is dead
                    if (REQUEST_HANDLING_THREADS[i] == null || !REQUEST_HANDLING_THREADS[i].isAlive()) {

                        //spawns a new thread to handle Client Request for Data Retrieving
                        (REQUEST_HANDLING_THREADS[i] = new DataRetrievalThread(clientSocket)).start();
                        break;
                    }
                }
                if (i == MAX_CLIENT_REQUESTS) {
                    //Tells the client that the server is not having any slot for serving his/her request
                    System.out.println(Thread.currentThread().getName() + " Server too busy. Try Later");
                    DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
                    dos.writeUTF(FileSystemConstants.DATA_SERVER_BUSY);
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * This is the main method to start the DataRetrievalRequestListener
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        new DataRetrievalRequestListener().listen();
    }
}
