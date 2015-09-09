package com.talentica.hungryHippos.accumulator;

/**
 * Created by debasishc on 9/9/15.
 */
public interface Work {
    void processRow(ExecutionContext executionContext);
    void calculate(ExecutionContext executionContext);
}
