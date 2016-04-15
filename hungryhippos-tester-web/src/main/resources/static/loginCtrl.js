'use strict';

var app=angular.module('loginApp',['ngResource','base64','ngRoute','ui.bootstrap','angular.filter','angularSpinner']);

app.factory("UserResource", function($resource) {
	return $resource("/user");
});

app.directive("limitTo", [function() {
    return {
        restrict: "A",
        link: function(scope, elem, attrs) {
        	limitCharactersInInputText(scope,elem,attrs);
        }
    }
}]);

app.controller('LoginCtrl',function ($scope,UserAccountService,$location,$window,usSpinnerService) {
	  $scope.user = {};
	  $scope.successDetails=null;
	  $scope.errorDetails=null;
	  
	  $scope.setError=function(error){
		  $scope.errorDetails=error;
		  $scope.successDetails=null;
	  };
	  
	  $scope.setSuccess=function(success){
		  $scope.successDetails=success;
		  $scope.errorDetails=null;
	  }
	  $scope.validateUser=function(ifValidCallback){
		  if($scope.user.password!=$scope.confirmPassword){
			  $scope.setError({message:"Password and confirm password should match. Please try again."});
			  $scope.user.password=null;
			  $scope.confirmPassword=null;
		  }else{
			  ifValidCallback();
		  }  
	  }
	  
	  $scope.resetUser=function(){
		  $scope.user = {};
		  $scope.confirmPassword=null;
	  }
	  
	  $scope.register=function(){
		  usSpinnerService.spin('spinner-1');
		  $scope.setError(null);
		  $scope.setSuccess(null);
		  $scope.validateUser(
				  function(){UserAccountService.saveNewUser(
						  	{"user":$scope.user},
						  	function(serviceResponse){
						  		if(serviceResponse && serviceResponse.error){
						  			$scope.setError(serviceResponse.error);
						  		}else{
						  			$scope.setSuccess({message:"Registration successful. Please login with your credentials."});
						  			 $scope.resetUser();
								 }
						  		usSpinnerService.stop('spinner-1');
						  	}
				  );
			  }
		  );
	  }
	  
	  $scope.loginUserName=null;
	  $scope.loginPassword=null;
	  $scope.loginErrorDetails={};
	  $scope.login=function(){
		  usSpinnerService.spin('spinner-1');
		  if($scope.loginUserName && $scope.loginPassword){
			  $scope.loginErrorDetails={};
			  UserAccountService.login($scope.loginUserName,$scope.loginPassword,
			  function(response){
		  		console.log("Logged in successfully");
		  		$window.location.href="/secure/welcome.html#/about";
		  		usSpinnerService.stop('spinner-1');
			  	},
			  	function(response){
			  		console.log("Log in failed.");
			  		$scope.loginErrorDetails.message="The email and password you entered don't match. Please try again.";
			  		usSpinnerService.stop('spinner-1');
			  	}
		  );
	    }else{
	    	$scope.loginErrorDetails.message="Please enter username and password both."
	    }
	  }
	  
});