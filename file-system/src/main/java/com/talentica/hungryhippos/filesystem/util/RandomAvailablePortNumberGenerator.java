package com.talentica.hungryhippos.filesystem.util;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.Random;

public class RandomAvailablePortNumberGenerator {

  private static final int START = 1035;
  private static final int END = 65535;

  /**
   * generate Random number Between 1024 6535
   * 
   * @return
   */
  public static int generateRandomNumber() {
    Random randomGenerator = new Random();
    return getRandomInteger(START, END, randomGenerator);
  }


  private static int getRandomInteger(int aStart, int aEnd, Random aRandom) {
    if (aStart > aEnd) {
      throw new IllegalArgumentException("Start cannot exceed End.");
    }
    // get the range, casting to long to avoid overflow problems
    long range = (long) aEnd - (long) aStart + 1;
    // compute a fraction of the range, 0 <= frac < range
    long fraction = (long) (range * aRandom.nextDouble());
    int randomNumber = (int) (fraction + aStart);
    return randomNumber;
  }

  public boolean checkPortFree(int portNumber) {
    // for tcp
    ServerSocket ss = null;
    // for UDP
    DatagramSocket ds = null;

    try {
      ss = new ServerSocket(portNumber);
      ss.setReuseAddress(true);
      ds = new DatagramSocket(portNumber);
      ds.setReuseAddress(true);
      return true;
    } catch (IOException e) {

    } finally {
      if (ds != null) {
        ds.close();
      }
      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          // this exception should not be thrown.

        }
      }
    }
    return false;
  }

}
