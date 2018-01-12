/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.node;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;

public class FileProviderService implements Runnable {

    private Socket socket;

    public FileProviderService(Socket socket) throws IOException {
        this.socket = socket;
    }

    @Override
    public void run() {
        DataInputStream dis = null;
        DataOutputStream dos = null;
        try {
            dis = new DataInputStream(socket.getInputStream());
            dos = new DataOutputStream(socket.getOutputStream());
            int bufferSize = 2048;
            byte[] buffer = new byte[bufferSize];
            while (dis.readBoolean()) {
                String filePath = dis.readUTF();
                File requestedFile = new File(filePath);
                long fileSize = requestedFile.length();
                dos.writeLong(fileSize);
                try (FileInputStream fis = new FileInputStream(requestedFile);
                     BufferedInputStream bis =
                             new BufferedInputStream(fis, bufferSize * 10);) {
                    int len;
                    while ((len = bis.read(buffer)) > -1) {
                        dos.write(buffer, 0, len);
                    }
                }
                dos.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                this.socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


}
