package com.talentica.hungryHippos.consumers.http;

/**
 * Created by santoshm1 on 6/9/15.
 */
public class LongEvent
{
    private String value;

    public void set(String value)
    {
        this.value = value;
    }

    public String get()
    {
        return this.value;
    }
}
