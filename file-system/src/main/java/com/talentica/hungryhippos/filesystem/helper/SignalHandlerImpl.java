package com.talentica.hungryhippos.filesystem.helper;

import java.util.Observable;

import sun.misc.Signal;

/**
 * {@code SignalHandlerImpl} used for handling the signal sent to hhfs console.
 * 
 * @author sudarshans
 *
 */
public class SignalHandlerImpl extends Observable implements sun.misc.SignalHandler {

  private Thread hhclThread = null;

  public void handleSignal(final String signalName, Thread mainThread)
      throws IllegalArgumentException {
    try {
      this.hhclThread = mainThread;
      sun.misc.Signal.handle(new sun.misc.Signal(signalName), this);
    } catch (IllegalArgumentException x) {
      // Most likely this is a signal that's not supported on this
      // platform or with the JVM as it is currently configured
      throw x;
    } catch (Throwable x) {
      // We may have a serious problem, including missing classes
      // or changed APIs
      throw new IllegalArgumentException("Signal unsupported: " + signalName, x);
    }
  }

  @Override
  public void handle(Signal arg0) {
    System.out.println("signal received is " + arg0);
    hhclThread.interrupt();

  }

}
