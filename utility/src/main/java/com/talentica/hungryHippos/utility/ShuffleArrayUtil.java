package com.talentica.hungryHippos.utility;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class ShuffleArrayUtil {
  
  public static void shuffleArray(Object[] ar)
  {
    Random rnd = ThreadLocalRandom.current();
    for (int i = ar.length - 1; i > 0; i--)
    {
      int index = rnd.nextInt(i + 1);
      Object a = ar[index];
      ar[index] = ar[i];
      ar[i] = a;
    }
  }
}
