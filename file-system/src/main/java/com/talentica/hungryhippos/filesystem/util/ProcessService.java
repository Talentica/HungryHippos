package com.talentica.hungryhippos.filesystem.util;

public interface ProcessService {
  boolean checkProcessIsAlive(Process process);
  void killProcess(Process process);
}
