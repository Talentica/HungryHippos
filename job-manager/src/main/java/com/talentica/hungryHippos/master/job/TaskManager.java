/**
 * 
 */
package com.talentica.hungryHippos.master.job;

import com.talentica.hungryHippos.sharding.Node;

/**
 * 
 * {@code TaskManager } manges the jobs for each nodes on the Priority Queue of Jobs.
 * 
 * @author PooshanS
 *
 */
public class TaskManager {

  private Node node;

  private JobPoolService jobPoolService;

  /**
   * creates an instance of TaskManager.
   */
  public TaskManager() {
    jobPoolService = new JobPoolService();
  }

  /**
   * retrieves the Node.
   * 
   * @return
   */
  public Node getNode() {
    return node;
  }

  /**
   * sets a Node.
   * 
   * @param node
   */
  public void setNode(Node node) {
    this.node = node;
  }

  /**
   * retrieves the JobPoolService.
   * 
   * @return
   */
  public JobPoolService getJobPoolService() {
    return jobPoolService;
  }

  /**
   * sets JobPoolService.
   * 
   * @param jobPoolService
   */
  public void setJobPoolService(JobPoolService jobPoolService) {
    this.jobPoolService = jobPoolService;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    Node node = (Node) o;
    return (node.getNodeId() == node.getNodeId());

  }

  @Override
  public int hashCode() {
    int result = TaskManager.class.hashCode() + node.getNodeId();
    return result;
  }

}
