package com.talentica.hungryHippos.accumulator;

import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

import java.nio.ByteBuffer;

/**
 * Created by debasishc on 9/9/15.
 */
public class ExecutionContextImpl implements ExecutionContext{
    private ByteBuffer data;
    private final DynamicMarshal dynamicMarshal;

    public ExecutionContextImpl(DynamicMarshal dynamicMarshal) {
        this.dynamicMarshal = dynamicMarshal;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }


    @Override
    public ByteBuffer getData() {
        return data;
    }

    @Override
    public Object getValue(int index) {
        return dynamicMarshal.readValue(index, data);
    }

    @Override
    public void saveValue(Object value) {
        System.out.println(value);
    }
}
