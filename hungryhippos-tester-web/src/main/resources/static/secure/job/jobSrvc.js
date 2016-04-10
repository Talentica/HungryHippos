'use strict';

app.service("JobService",function(NewJobResource,JobHistoryResource,JobDetailResource,JobStatusResource) {
	this.submitNewJob = function(jobDetail,callback){
		var jobServiceRequest = {"jobDetail" :jobDetail}
		NewJobResource.save(jobServiceRequest,callback);
	}
	
	this.getRecentJobs = function(userIdParam,callback){
		JobHistoryResource.get({userId:userIdParam},callback);
	}
	
	this.getJobStatusDetail=function(uuid,callback){
		JobStatusResource.get({jobUuid:uuid},callback);
	}
	
});