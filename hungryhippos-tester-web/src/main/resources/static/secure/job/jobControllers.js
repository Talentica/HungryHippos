'use strict';

app.controller('NewJobCtrl',function ($scope,JobService,usSpinnerService,FileUploader) {
	$scope.jobDetail={};
	$scope.error={};
	$scope.numberOfColumnsInDataFile=9;
	$scope.notification={};
	$scope.jobJarFile=new FileUploader();

	$scope.getArrayOfSize=function(size){
		return new Array(size);
	}
	$scope.dataTypeConfiguration= [{"shardingDimension":true,"dataType":"STRING","dataSize":2},{"dataType":"STRING","dataSize":2},{"dataType":"STRING","dataSize":2},{"dataType":"STRING","dataSize":3},{"dataType":"STRING","dataSize":3},{"dataType":"STRING","dataSize":3},{"dataType":"DOUBLE"},{"dataType":"DOUBLE"},{"dataType":"STRING","dataSize":5}]
	
	$scope.updateDataTypeConfiguration=function(){
		$scope.dataTypeConfiguration= [{"shardingDimension":true,"dataType":"STRING","dataSize":2},{"dataType":"STRING","dataSize":2},{"dataType":"STRING","dataSize":2},{"dataType":"STRING","dataSize":3},{"dataType":"STRING","dataSize":3},{"dataType":"STRING","dataSize":3},{"dataType":"DOUBLE"},{"dataType":"DOUBLE"},{"dataType":"STRING","dataSize":5}]
//		$scope.dataTypeConfiguration= $scope.getArrayOfSize($scope.numberOfColumnsInDataFile);
	}
	
	$scope.getDataTypeConfiguration=function(){
		return $scope.dataTypeConfiguration;
	}
	
	$scope.createNewJob=function(){
		usSpinnerService.spin('spinner-1');
		$scope.error={};
		$scope.notification={};
		var shardingDimensions="";
		var shardingDimensionsSelected=false;
		var dataTypeConfigSingleString="";
		for(var i=0;i<$scope.dataTypeConfiguration.length;i++){
			if($scope.dataTypeConfiguration[i] && $scope.dataTypeConfiguration[i].dataType){
				if($scope.dataTypeConfiguration[i].dataType==='STRING' && !$scope.dataTypeConfiguration[i].dataSize){
					$scope.error.message="Please select maximum no. of characters for a 'String' data type column.";
					usSpinnerService.stop('spinner-1');
					return;
				}
				if(i>0){
					dataTypeConfigSingleString=dataTypeConfigSingleString+",";
				}
				if(shardingDimensionsSelected && $scope.dataTypeConfiguration[i].shardingDimension){
					shardingDimensions=shardingDimensions+",";
				}
				dataTypeConfigSingleString=dataTypeConfigSingleString+$scope.dataTypeConfiguration[i].dataType+'-'+($scope.dataTypeConfiguration[i].dataSize||0);
				if($scope.dataTypeConfiguration[i].shardingDimension){
					shardingDimensions=shardingDimensions+"key"+(i+1);
					shardingDimensionsSelected=true;
				}
			}else{
				$scope.error.message="Please select data type of all columns.";
				usSpinnerService.stop('spinner-1');
				return;
			}
		}
		if(!shardingDimensionsSelected){
			$scope.error.message="Please select at least one sharding dimension.";
			usSpinnerService.stop('spinner-1');
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
		        						usSpinnerService.stop('spinner-1');
		        					}else if(response.error){
		        						$scope.error.message="There was some error occurred on server side. Please try again."
		        						usSpinnerService.stop('spinner-1');
		        					}else{
		        						var jobuuid= $scope.jobDetail.uuid;
		        						$scope.reset();
		        						$scope.notification.message="Job with id: "+jobuuid+" submitted successfully.";
		        					}
		        					usSpinnerService.stop('spinner-1');
		        				}
		        		);
		        	}else{
		        		$scope.error.message=(response.error && response.error.message)||"File upload failed. Please logout and login again and try after some time.";
		        		usSpinnerService.stop('spinner-1');
		        	}
		        }, 
		        function(response){
		        	$scope.error.message=response||"File upload failed.";
		        	usSpinnerService.stop('spinner-1');
		        }
		);
	}
	
	 $scope.uploadJobJarFile = function(sucessCallback,errorCallback){
	        var file = $scope.jobJarFile;
	        if(file && file.queue && file.queue.length>0 && file.queue[file.queue.length-1] && file.queue[file.queue.length-1]._file){
	        JobService.uploadJobJarFile(file.queue[file.queue.length-1]._file,$scope.jobDetail.jobInput.jobMatrixClass,sucessCallback,errorCallback);
	        }else{
	        	$scope.error.message="Please select a job jar file.";
	        	usSpinnerService.stop('spinner-1');
	        }
	};
	
	$scope.reset=function(){
		$scope.error={};
		$scope.numberOfColumnsInDataFile=9;
		$scope.notification={};
		$scope.jobDetail={};
//		$scope.dataTypeConfiguration=$scope.getArrayOfSize(1);
		$scope.dataTypeConfiguration= [{"shardingDimension":true,"dataType":"STRING","dataSize":2},{"dataType":"STRING","dataSize":2},{"dataType":"STRING","dataSize":2},{"dataType":"STRING","dataSize":3},{"dataType":"STRING","dataSize":3},{"dataType":"STRING","dataSize":3},{"dataType":"DOUBLE"},{"dataType":"DOUBLE"},{"dataType":"STRING","dataSize":5}]
		$scope.jobJarFile.clearQueue();
		usSpinnerService.stop('spinner-1');
		$("#jobJarFile").value=null;
	}
	
});


app.controller('JobHistoryCtrl',function ($scope,JobService,usSpinnerService) {
	$scope.jobsPresent=false;
	$scope.jobsLoaded=false;
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

	usSpinnerService.spin('spinner-1');
	JobService.getRecentJobs(function(response){
		if(response && response.jobs && response.jobs.length){
			 $scope.recentJobs = [];
			 angular.copy(response.jobs, $scope.recentJobs);
			 $scope.recentJobs = $scope.groupByDayWise($scope.recentJobs, 'dateTimeSubmitted');
			 $scope.jobsPresent=true;
		}else{
			$scope.recentJobs =null;
			$scope.jobsPresent=false;
		}
		$scope.jobsLoaded=true;
		usSpinnerService.stop('spinner-1');
	});
	
	$scope.jobStepInformation=null;
	$scope.getJobStatusDetail=function(jobUuid){
		if(!$scope.jobStepInformation){
			usSpinnerService.spin('spinner-1');
			$scope.jobStepInformation=[];
			JobService.getJobStatusDetail(jobUuid,function(response){
				if(response && response.jobDetail && response.jobDetail.uuid && response.processInstances){
				$scope.jobStepInformation[response.jobDetail.uuid]=response;
				}
				usSpinnerService.stop('spinner-1');
			}
		);
		}
	}
});