package com.talentica.hungryHippos.storage;

import java.nio.ByteBuffer;

import com.talentica.hungryHippos.client.job.Job;

/**
 * Created by debasishc on 31/8/15.
 */
public interface RowProcessor {
    void processRow(ByteBuffer row);
    void finishUp();
    Job getJob();
    void processRowCount(ByteBuffer row);
}
