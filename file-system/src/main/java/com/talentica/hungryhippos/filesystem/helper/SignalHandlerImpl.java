/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
