'use strict';

var app=angular.module('loginApp',['ngResource','base64','ngRoute','ui.bootstrap','angular.filter']);

app.factory("UserResource", function($resource) {
	return $resource("/user");
});

app.controller('LoginCtrl',function ($scope,UserAccountService,$location,$window) {
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
						  	}
				  );
			  }
		  );
	  }
	  
	  $scope.loginUserName=null;
	  $scope.loginPassword=null;
	  $scope.loginErrorDetails={};
	  $scope.login=function(){
		  if($scope.loginUserName && $scope.loginPassword){
			  $scope.loginErrorDetails={};
			  UserAccountService.login($scope.loginUserName,$scope.loginPassword,
			  function(response){
		  		console.log("Logged in successfully");
		  		$window.location.href="/secure/welcome.html#/about";
			  	},
			  	function(response){
			  		console.log("Log in failed.");
			  		$scope.loginErrorDetails.message="The email and password you entered don't match. Please try again.";
			  	}
		  );
	    }else{
	    	$scope.loginErrorDetails.message="Please enter username and password both."
	    }
	  }
	  
});