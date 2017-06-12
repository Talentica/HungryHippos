package com.talentica.hungryHippos.node.joiners;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Created by rajkishoreh on 19/5/17.
 */
@FunctionalInterface
public interface CallableGenerator {
    Callable<Boolean> getCallable(Queue<String> fileSrcQueue, String hhFilePath);
}
