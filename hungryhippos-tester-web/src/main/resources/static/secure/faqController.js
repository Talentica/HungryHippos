'use strict';

app.controller('FaqCtrl',function ($scope,EnvironmentSrvc) {
	
	$(".nav").find(".active").removeClass("active");
	$("#faqLink").parent().addClass("active");
	
	EnvironmentSrvc.getvalueof("clientapidownloadlocation",
			function(response){
				if(response && response.value){
					$scope.client_jar_location=response.value;
				}
			}
	);
	
	EnvironmentSrvc.getvalueof("testjobsjarfilelocation",
			function(response){
				if(response && response.value){
					$scope.test_job_matrix_jar_location=response.value;
				}
			}
	);
	
	EnvironmentSrvc.getvalueof("clientapidocslocation",
			function(response){
				if(response && response.value){
					$scope.client_api_documentation=response.value;
				}
			}
	);
});