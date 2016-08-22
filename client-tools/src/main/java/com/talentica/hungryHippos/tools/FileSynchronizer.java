package com.talentica.hungryHippos.tools;

import com.talentica.hungryHippos.tools.clients.FileSyncUpClient;

import java.io.IOException;

/**
 * This class is for synchronizing the files among nodes
 * Created by rajkishoreh on 22/8/16.
 */
public class FileSynchronizer {

    /**
     * Syncs up the file across nodes
     * @param filePathForSyncUp
     * @param hostIP
     * @throws IOException
     */
    public static void syncUpFileAcrossNodes(String filePathForSyncUp, String hostIP) throws IOException {
        FileSyncUpClient fileSyncUpClient = new FileSyncUpClient(filePathForSyncUp);
        fileSyncUpClient.sendRequest(hostIP);
    }

}
