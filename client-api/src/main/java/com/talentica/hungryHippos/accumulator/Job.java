package com.talentica.hungryHippos.accumulator;

/**
 * Created by debasishc on 9/9/15.
 */
public interface Job {
    Work createNewWork();
    int[] getDimensions();
    int getPrimaryDimension();
    int getJobId();
    void status(String status);
    long getMemoryFootprint(int rowCount);
}