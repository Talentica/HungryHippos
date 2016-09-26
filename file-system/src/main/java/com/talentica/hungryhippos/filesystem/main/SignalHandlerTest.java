package com.talentica.hungryhippos.filesystem.main;

import java.util.Observable;
import java.util.Observer;

public class SignalHandlerTest implements Observer {

  public static void main(final String[] args) {
    new SignalHandlerTest().go();
  }

  private void go() {
    final SignalHandler sh = new SignalHandler();
    sh.addObserver(this);
    //sh.handleSignal("TERM");
  //  sh.handleSignal("INT");
    System.out.println("Sleeping for 10 seconds:- hit me with signals");
    try {
      Thread.sleep(50000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }


  }

  @Override
  public void update(Observable o, Object arg) {
    System.out.println("Received signal:- " + arg);

  }


}
