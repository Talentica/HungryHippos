package com.talentica.hungryHippos.accumulator;

import com.talentica.hungryHippos.utility.marshaling.MutableCharArrayString;

import java.nio.ByteBuffer;

/**
 * Created by debasishc on 9/9/15.
 */
public interface ExecutionContext {
    ByteBuffer getData();
    Object getValue(int index);
    MutableCharArrayString getString(int index);
    void saveValue(Object value);
    void setKeys(ValueSet valueSet);
    ValueSet getKeys();
}