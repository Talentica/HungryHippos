'use strict';

app.service("JobService",function($http,JobHistoryResource,JobDetailResource,JobStatusResource,JobOutputResource) {
	this.submitNewJob = function(jobDetail,callback){
		var jobServiceRequest = {"jobDetail" :jobDetail}
    	$http.post("/job/new",jobServiceRequest).success(callback).error(callback);
	}
	
	this.getRecentJobs = function(userIdParam,callback){
		JobHistoryResource.get({userId:userIdParam},callback);
	}
	
	this.getJobStatusDetail=function(uuid,callback){
		JobStatusResource.get({jobUuid:uuid},callback);
	}
	
	this.getJobOutputDetail =function(uuid,callback){
		JobOutputResource.get({jobUuid:uuid},callback);
	}
	
    this.uploadJobJarFile= function(file,successCallback,errorCallback){
            var fd = new FormData();
            fd.append('file', file);
            $http.post("/job/jar/upload",fd, {
                transformRequest: angular.identity,
                headers: {'Content-Type': undefined}
            }).success(successCallback).error(errorCallback);
    };
});
