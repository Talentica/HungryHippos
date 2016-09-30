package com.talentica.hungryHippos.common;

import java.io.Serializable;

import com.talentica.hungryHippos.utility.JobEntity;

public interface JobRunner extends Serializable {

  public void run(JobEntity jobEntity);

}
