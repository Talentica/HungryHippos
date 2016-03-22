'use strict';

var app=angular.module('hungryhipposTesterWebappAngularApp',['ngResource'])

app.controller('LoginCtrl', ['$scope', '$resource' ,function ($scope,$resource) {
	  $scope.user = {};
	  $scope.register=function(){
		  var user= $resource("http://localhost:8080/user");
		  user.save($scope.user,function(response) {
			  console.log('User information successfully saved');
			  console.log('Response is:'+response);
			  $scope.user=response;
		  })
	  }
}]);