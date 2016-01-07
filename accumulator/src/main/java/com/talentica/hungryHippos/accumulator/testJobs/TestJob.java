package com.talentica.hungryHippos.accumulator.testJobs;

import java.io.Serializable;

import com.talentica.hungryHippos.accumulator.Job;
import com.talentica.hungryHippos.accumulator.Work;

/**
 * Created by debasishc on 9/9/15.
 */
public class TestJob implements Job,Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = -4111336293020419218L;
	protected int [] dimensions;
    protected int primaryDimension;
    protected int valueIndex;
    protected int jobId;
    protected long dataSize;
    protected String status;
    private int rowCount = 0;
    public TestJob(){}
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
	public void status(String status) {
		this.status = status;
	}
	/*@Override
	public void addDataSize(long dataSize) {
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

	@Override
	public int incrRowCount() {
		return ++rowCount ;
	}*/
	@Override
	public long getMemory(int rowCount) {
		// TODO Auto-generated method stub
		return 0;
	}

}
