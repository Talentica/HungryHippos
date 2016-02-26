package com.talentica.hungryHippos.client.job;

import com.talentica.hungryHippos.client.domain.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public interface Job {
    Work createNewWork();
    int[] getDimensions();
    int getPrimaryDimension();
    int getIndex();
    long getMemoryFootprint(int rowCount);
}
