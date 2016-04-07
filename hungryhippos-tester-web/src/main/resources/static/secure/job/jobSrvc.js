'use strict';

app.service("JobService",function(NewJobResource,JobHistoryResource,JobDetailResource) {
	this.submitNewJob = function(jobDetail,callback){
		var jobServiceRequest = {"jobDetail" :jobDetail}
		NewJobResource.save(jobServiceRequest,callback);
	}
	
	this.getRecentJobs = function(userIdParam,callback){
		JobHistoryResource.get({userId:userIdParam},callback);
	}
	
});