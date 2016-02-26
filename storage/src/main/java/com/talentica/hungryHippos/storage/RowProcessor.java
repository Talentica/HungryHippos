package com.talentica.hungryHippos.storage;

import java.nio.ByteBuffer;

import com.talentica.hungryHippos.utility.JobEntity;

/**
 * Created by debasishc on 31/8/15.
 */
public interface RowProcessor {
    void processRow(ByteBuffer row);
    void finishUp();
    JobEntity getJobEntity();
    void processRowCount(ByteBuffer row);
}
