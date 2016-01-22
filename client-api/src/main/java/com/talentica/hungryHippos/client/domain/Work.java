package com.talentica.hungryHippos.client.domain;

/**
 * Created by debasishc on 9/9/15.
 */
public interface Work {
	String getWorkerId();
    void processRow(ExecutionContext executionContext);
    void calculate(ExecutionContext executionContext);
    void incrCountRow();
    int getRowCount();
}
