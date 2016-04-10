'use strict';

app.controller('NewJobCtrl',function ($scope,JobService) {
	$scope.job={
		    "job" : null,
		    "jobInput" : {
		      "jobInputId" : null,
		      "jobId" : null,
		      "dataLocation" : "http://google.com",
		      "dataSize" : 0,
		      "dataTypeConfiguration" : "STRING-1,STRING-1,STRING-1,STRING-3,STRING-3,STRING-3,DOUBLE-0,DOUBLE-0,STRING-5",
		      "shardingDimensions" : "key1,key2,key3"
		    },
		    "jobOutput" : null,
		    "executionTimeInSeconds" : null
		  };
});


app.controller('JobHistoryCtrl',function ($scope,JobService) {

	$scope.groupByDayWise = function (arr, key) {
	    var groups = {};
	    for (var i=0;i<arr.length;i++) {
          if(!arr[i][key].toLocaleDateString){
        	  arr[i][key]=new Date(arr[i][key]);
          }
          arr[i][key]['day'] = arr[i][key].toDateString();
	      groups[arr[i][key]['day']] = groups[arr[i][key]['day']] || [];
     	  groups[arr[i][key]['day']].push(arr[i]);
	    }
	    return groups;
	};	
	
	JobService.getRecentJobs(1,function(response){
		if(response && response.jobs){
			 $scope.recentJobs = [];
			 angular.copy(response.jobs, $scope.recentJobs);
			 $scope.recentJobs = $scope.groupByDayWise($scope.recentJobs, 'dateTimeSubmitted');
		}
	});
	
	$scope.jobStepInformation=null;
	$scope.getJobStatusDetail=function(jobUuid){
		if(!$scope.jobStepInformation){
			$scope.jobStepInformation=[];
			JobService.getJobStatusDetail(jobUuid,function(response){
				if(response && response.jobDetail.job && response.jobDetail.job.uuid && response.processInstances){
				$scope.jobStepInformation[response.jobDetail.job.uuid]=response;
				}
			}
		);
		}
	}
});