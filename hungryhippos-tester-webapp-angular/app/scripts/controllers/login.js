'use strict';

angular.module('hungryhipposTesterWebappAngularApp',['base64','ngResource'])
  .controller('LoginCtrl', ['$base64', '$scope' ,function ($base64, $scope,$resource) {
	  var userResource= $resource('http://localhost:8080/user');
	  var user= userResource.get({emailAddress:'nitin.kasat@talentica.com'}, function(data) {
          $scope.user = data;
      });
  }]);