'use strict';

app.controller('LoginCtrl',function ($scope,UserAccountService) {
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
});