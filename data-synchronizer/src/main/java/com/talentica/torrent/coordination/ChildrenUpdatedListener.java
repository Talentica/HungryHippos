package com.talentica.torrent.coordination;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ChildrenUpdatedListener implements PathChildrenCacheListener {

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Logger LOGGER = LoggerFactory.getLogger(ChildrenUpdatedListener.class);


  @SuppressWarnings("resource")
  protected static void register(CuratorFramework client, String nodePathToListenTo,
      PathChildrenCacheListener listener) {
    PathChildrenCache childrenCache = null;
    try {
      createListenerNodeIfDoesntExist(client, nodePathToListenTo);
      childrenCache = new PathChildrenCache(client, nodePathToListenTo, true);
      childrenCache.getListenable().addListener(listener);
      childrenCache.start(StartMode.POST_INITIALIZED_EVENT);
      LOGGER.info("Listener registered successfully for path:" + nodePathToListenTo);
    } catch (Exception exception) {
      LOGGER.error("Error occurred while registering NewTorrentAvailableListener.", exception);
      closeChildrenCache(childrenCache);
      throw new RuntimeException(exception);
    }
  }

  private static void closeChildrenCache(PathChildrenCache childrenCache) {
    try {
      if (childrenCache != null) {
        childrenCache.close();
      }
    } catch (Exception exception) {
      LOGGER.error("Error occurred while closing path children cache.", exception);
      throw new RuntimeException(exception);
    }
  }

  private static void createListenerNodeIfDoesntExist(CuratorFramework client,
      String zkNodeToListenTo) throws Exception {
    try {
      boolean pathDoesntExist = client.checkExists().forPath(zkNodeToListenTo) == null;
      if (pathDoesntExist) {
        client.create().creatingParentsIfNeeded().forPath(zkNodeToListenTo);
      }
    } catch (NodeExistsException exception) {
      LOGGER.warn("Node already exists: {}", zkNodeToListenTo);
    }
  }

  protected boolean checkIfNodeAddedOrUpdated(PathChildrenCacheEvent event) {
    return event.getType() == Type.CHILD_ADDED || event.getType() == Type.CHILD_UPDATED;
  }

  protected void cleanup(CuratorFramework client, String path) {
    try {
      if (path != null && client.checkExists().forPath(path) != null) {
        client.delete().forPath(path);
      }
    } catch (Exception exception) {
      LOGGER.error("Error occurred while deleting node:" + path, exception);
    }
  }

  protected void handleError(CuratorFramework client, String path, Exception error,
      String errorNodeBasePath) {
    try {
      LOGGER.error(
          "Error occurred while processing request to seed file for node with path: " + path,
          error);
      if (path != null) {
        client.create().creatingParentsIfNeeded()
            .forPath(errorNodeBasePath + StringUtils.substringAfterLast(path, "/"));
      }
    } catch (Exception exception) {
      LOGGER.error("Error occurred while deleting node:" + path, exception);
    }
  }

}
