package com.talentica.hungryhippos.filesystem;

/**
 * This class contains the constants for FileSystem
 * Created by rajkishoreh on 1/7/16.
 */
public interface FileSystemConstants {

    String DATA_SERVER_BUSY = "DATA_SERVER_BUSY";
    String DATA_SERVER_AVAILABLE = "DATA_SERVER_AVAILABLE";
    String DATA_TRANSFER_COMPLETED = "DATA_TRANSFER_COMPLETED";
    String FILE_PATHS_DELIMITER = ",";
    String DOWNLOAD_FILE_PREFIX = "part-";

    //constants for file-system configuration
    String HHROOT = "file.system.root.directory";
    String DATA_FILE_PREFIX = "file.system.data.file.prefix";
    String ROOT_NODE = "zookeeper.file.system";
    String SERVER_PORT = "file.system.server.port";
    String MAX_CLIENT_REQUESTS= "file.system.max.client.requests";
    String MAX_QUERY_ATTEMPTS="file.system.max.query.attempts";
    String QUERY_RETRY_INTERVAL = "file.system.query.retry.interval";
    String FILE_STREAM_BUFFER_SIZE = "file.system.file.stream.buffer.size";

}

