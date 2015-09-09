package com.talentica.hungryHippos.accumulator;

import java.nio.ByteBuffer;

/**
 * Created by debasishc on 9/9/15.
 */
public interface ExecutionContext {
    ByteBuffer getData();
    Object getValue(int index);
    void saveValue(Object value);
}
