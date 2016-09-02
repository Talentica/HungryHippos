package com.talentica.hungryhippos.filesystem.util;

public class ProcessServiceImpl implements ProcessService {


  @Override
  public boolean checkProcessIsAlive(Process process) {
    // ps -o pid= -p 29555
    boolean isAlive = true;
    String userName = process.getUserName();
    String host = process.getName();
    String pid = process.getProcessId();
    String command = "ps -o pid= -p " + pid;

    ProcessBuilder pb = JavaProcessBuilder.constructProcessBuilder(true, userName, host, command);
    int count = JavaProcessBuilder.execute(pb);
    if (count != 0) {
      isAlive = false;
      process.setIsAlive(isAlive);
    }
    return isAlive;
  }

  @Override
  public void killProcess(Process process) {

    String userName = process.getUserName();
    String host = process.getName();
    String pid = process.getProcessId();
    String command = "kill -9 " + pid;

    boolean isAlive = checkProcessIsAlive(process);
    if (isAlive) {
      ProcessBuilder pb = JavaProcessBuilder.constructProcessBuilder(true, userName, host, command);
      int count = JavaProcessBuilder.execute(pb);
      if (count == 0) {
        isAlive = false;
        process.setIsAlive(isAlive);
      }
    } else {
      System.out.println("process is not alive");
      return;
    }
    System.out.println("Process is killed successfuly");
  }


}
