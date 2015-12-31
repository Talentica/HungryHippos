package com.talentica.hungryHippos.storage;

import java.nio.ByteBuffer;

/**
 * Created by debasishc on 31/8/15.
 */
public interface RowProcessor {
    void processRow(ByteBuffer row);
    void finishUp();
    //void rowCount(ByteBuffer row);
    //void finishRowCount();
    //Map<Integer,Integer> getTotalRowCountByJobId();
    void incrRowCount();
    Object getJob();
    Long totalRowCount();
}
