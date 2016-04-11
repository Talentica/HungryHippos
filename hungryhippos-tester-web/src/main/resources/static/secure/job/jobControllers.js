'use strict';

app.controller('NewJobCtrl',function ($scope,JobService) {
	$scope.jobDetail={};
	$scope.numberOfColumnsInDataFile=1;
	$scope.getArrayOfSize=function(size){
		return new Array(size);
	}
	$scope.dataTypeConfiguration=$scope.getArrayOfSize(1);
	
	$scope.updateDataTypeConfiguration=function(){
		$scope.dataTypeConfiguration= $scope.getArrayOfSize($scope.numberOfColumnsInDataFile);
	}
	
	$scope.getDataTypeConfiguration=function(){
		return $scope.dataTypeConfiguration;
	}
	
	$scope.createNewJob=function(){
		var dataTypeConfigSingleString="";
		for(var i=0;i<$scope.dataTypeConfiguration.length;i++){
			if(i>0){
				dataTypeConfigSingleString=dataTypeConfigSingleString+",";
			}
			dataTypeConfigSingleString=dataTypeConfigSingleString+$scope.dataTypeConfiguration[i].dataType+'-'+($scope.dataTypeConfiguration[i].dataSize||0);
		}
		console.log(dataTypeConfigSingleString);
		$scope.uploadJobJarFile(
				function(response){
		        	if(!response.error && response.uploadedFileSize>0){
		        		console.log("Successfully uploaded. Job id is: "+response.jobUuid);
		        	}else{
		        		console.log("File upload failed.");
		        	}
		        }, 
		        function(response){
		        	console.log("File upload failed.");
		        }
		);
	}
	
	 $scope.uploadJobJarFile = function(sucessCallback,errorCallback){
	        var file = $scope.jobJarFile;
	        JobService.uploadJobJarFile(file,sucessCallback,errorCallback);
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
				if(response && response.jobDetail && response.jobDetail.uuid && response.processInstances){
				$scope.jobStepInformation[response.jobDetail.uuid]=response;
				}
			}
		);
		}
	}
});