'use strict';

app.controller('NewJobCtrl',function ($scope,JobService) {
	$scope.jobDetail={};
	$scope.error={};
	$scope.numberOfColumnsInDataFile=1;
	$scope.notification={};

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
		$scope.error={};
		$scope.notification={};
		var shardingDimensions="";
		var shardingDimensionsSelected=false;
		var dataTypeConfigSingleString="";
		for(var i=0;i<$scope.dataTypeConfiguration.length;i++){
			if($scope.dataTypeConfiguration[i] && $scope.dataTypeConfiguration[i].dataType){
				if($scope.dataTypeConfiguration[i].dataType==='STRING' && !$scope.dataTypeConfiguration[i].dataSize){
					$scope.error.message="Please select maximum no. of characters for a 'String' data type column.";
					return;
				}
				if(i>0){
					dataTypeConfigSingleString=dataTypeConfigSingleString+",";
				}
				if(shardingDimensionsSelected){
					shardingDimensions=shardingDimensions+",";
				}
				dataTypeConfigSingleString=dataTypeConfigSingleString+$scope.dataTypeConfiguration[i].dataType+'-'+($scope.dataTypeConfiguration[i].dataSize||0);
				if($scope.dataTypeConfiguration[i].shardingDimension){
					shardingDimensions=shardingDimensions+"key"+(i+1);
					shardingDimensionsSelected=true;
				}
			}else{
				$scope.error.message="Please select data type of all columns.";
				return;
			}
		}
		if(!shardingDimensionsSelected){
			$scope.error.message="Please select at least one sharding dimension.";
			return;
		}else{
			$scope.jobDetail.jobInput.shardingDimensions=shardingDimensions;
		}
		$scope.jobDetail.jobInput.dataTypeConfiguration=dataTypeConfigSingleString;
		$scope.uploadJobJarFile(
				function(response){
		        	if(!response.error && response.uploadedFileSize>0){
		        		$scope.jobDetail.uuid=response.jobUuid;
		        		$scope.notification={};
		        		$scope.error={};
		        		JobService.submitNewJob($scope.jobDetail,
		        				function(response){
		        					if(response && response.error && response.error.message){
		        						$scope.error.message=response.error.message;
		        					}else if(response.error){
		        						$scope.error.message="There was some error occurred on server side. Please try again."
		        					}else{
		        						$scope.reset();
		        						$scope.notification.message="Job with id: "+$scope.jobDetail.uuid+" submitted successfully.";
		        					}
		        				}
		        		);
		        	}else{
		        		$scope.error.message=(response.error.message)||"File upload failed.";
		        	}
		        }, 
		        function(response){
		        	$scope.error.message=response||"File upload failed.";
		        }
		);
	}
	
	 $scope.uploadJobJarFile = function(sucessCallback,errorCallback){
	        var file = $scope.jobJarFile;
	        JobService.uploadJobJarFile(file,$scope.jobDetail.jobInput.jobMatrixClass,sucessCallback,errorCallback);
	};
	
	$scope.reset=function(){
		$scope.error={};
		$scope.numberOfColumnsInDataFile=1;
		$scope.notification={};
	}
	
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
		if(response && response.jobs && response.jobs.length){
			 $scope.recentJobs = [];
			 angular.copy(response.jobs, $scope.recentJobs);
			 $scope.recentJobs = $scope.groupByDayWise($scope.recentJobs, 'dateTimeSubmitted');
		}else{
			$scope.recentJobs =null;
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