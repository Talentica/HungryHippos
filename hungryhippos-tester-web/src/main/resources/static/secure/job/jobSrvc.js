'use strict';

app.service("JobService",function($http,JobHistoryResource,JobDetailResource,JobStatusResource,JobOutputResource) {
	this.submitNewJob = function(jobDetail,callback){
		var jobServiceRequest = {"jobDetail" :jobDetail}
    	$http.post("/job/new",jobServiceRequest).success(callback).error(callback);
	}
	
	this.getRecentJobs = function(callback){
		JobHistoryResource.get(callback);
	}
	
	this.getJobStatusDetail=function(uuid,callback){
		JobStatusResource.get({jobUuid:uuid},callback);
	}
	
	this.getJobOutputDetail =function(uuid,callback){
		JobOutputResource.get({jobUuid:uuid},callback);
	}
	
    this.uploadJobJarFile= function(file,jobMatrixClassName,dataParserClassName,successCallback,errorCallback){
            var fd = new FormData();
            fd.append('file', file);
            fd.append("dataParserClassName",dataParserClassName);
            fd.append("jobMatrixClassName",jobMatrixClassName);
            $http.post("/secure/job/jar/upload",fd, {
                transformRequest: angular.identity,
                headers: {'Content-Type': undefined}
            }).success(successCallback).error(errorCallback);
    };
});
