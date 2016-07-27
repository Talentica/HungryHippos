package com.talentica.hungryhippos.filesystem.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class RandomAvailablePortNumberGeneratorTest {

  @Test
  public void testGenerateRandomNumber() {
    int portNumber = RandomAvailablePortNumberGenerator.generateRandomNumber();
    assertTrue((portNumber > 1035 && portNumber < 65535));
  }

}
