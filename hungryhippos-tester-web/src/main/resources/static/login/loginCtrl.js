'use strict';

app.controller('LoginCtrl',function ($scope,UserAccountService) {
	  $scope.user = {};
	  $scope.register=function(){
		  UserAccountService.saveNewUser($scope.user);
	  }
});