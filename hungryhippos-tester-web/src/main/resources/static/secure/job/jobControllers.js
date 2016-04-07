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
	JobService.getRecentJobs(1,function(response){
		if(response && response.jobs){
			$scope.recentJobs=response.jobs;
		}
	});
});