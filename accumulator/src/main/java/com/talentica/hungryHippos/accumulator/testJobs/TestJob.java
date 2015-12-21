package com.talentica.hungryHippos.accumulator.testJobs;

import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.accumulator.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class TestJob implements Job{
    protected int [] dimensions;
    protected int primaryDimension;
    protected int valueIndex;
    protected int jobId;
    protected long dataSize;
    protected String status;
    public TestJob(int[] dimensions, int primaryDimension, int valueIndex, int jobId) {
        this.dimensions = dimensions;
        this.primaryDimension = primaryDimension;
        this.valueIndex = valueIndex;
        this.jobId = jobId;
    }


    @Override
    public Work createNewWork() {
        return new TestWork(dimensions,primaryDimension,valueIndex);
    }

    @Override
    public int[] getDimensions() {
        return dimensions;
    }

    @Override
    public int getPrimaryDimension() {
        return primaryDimension;
    }

	@Override
	public int getJobId() {
		return jobId;
	}

	@Override
	public void putDataSize(long dataSize) {
		this.dataSize = dataSize;
	}

	@Override
	public long getDataSize() {
		return this.dataSize;
	}

	@Override
	public void status(String status) {
		this.status = status;
	}
}
