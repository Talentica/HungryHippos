'use strict';

var app=angular.module('testerWebApp',['ngResource','base64','ngRoute','ui.bootstrap','angular.filter']);

app.factory("UserResource", function($resource) {
	return $resource("/user");
});

app.factory("NewJobResource", function($resource) {
	return $resource("/job/new");
});

app.factory("JobHistoryResource", function($resource) {
	return $resource("/job/history/:userId");
});

app.factory("JobDetailResource", function($resource) {
	return $resource("/job/detail/:jobUuid");
});

app.factory("JobStatusResource", function($resource) {
	return $resource("/job/status/:jobUuid");
});

app.factory("JobOutputResource", function($resource) {
	return $resource("/job/output/detail/:jobUuid");
});


app.directive('fileModel', ['$parse', function ($parse) {
    return {
        restrict: 'A',
        link: function(scope, element, attrs) {
            var model = $parse(attrs.fileModel);
            var modelSetter = model.assign;
            
            element.bind('change', function(){
                scope.$apply(function(){
                    modelSetter(scope, element[0].files[0]);
                });
            });
        }
    };
}]);

app.config(function($routeProvider, $locationProvider) {
	  $routeProvider
	  .when('/login', {
	    templateUrl: '/login/login.html',
	    controller: 'LoginCtrl'
	  })
	  .when('/newjob', {
	    templateUrl: 'secure/job/newjob.html',
	    controller: 'NewJobCtrl'
	  })
	  .when('/history', {
	    templateUrl: 'secure/job/jobHistory.html',
	    controller: 'JobHistoryCtrl'
	  })
	  
});