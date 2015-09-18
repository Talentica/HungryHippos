package com.talentica.hungryHippos.accumulator;

import com.talentica.hungryHippos.utility.marshaling.DynamicMarshal;

import java.io.PrintStream;
import java.nio.ByteBuffer;

/**
 * Created by debasishc on 9/9/15.
 */
public class ExecutionContextImpl implements ExecutionContext{
    private ByteBuffer data;
    private final DynamicMarshal dynamicMarshal;
    private ValueSet keys;

    private static PrintStream out;

    static{
        try{
            out = new PrintStream("outputFile");
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

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
        out.println(keys.toString() + " ==> " + value);
    }

    @Override
    public void setKeys(ValueSet valueSet) {
        this.keys = valueSet;
    }

    @Override
    public ValueSet getKeys() {
        return keys;
    }
}
