'use strict';

var app=angular.module('testerWebApp',['ngResource','base64','ngRoute','ui.bootstrap']);

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

app.config(function($routeProvider, $locationProvider) {
	  $routeProvider
	   .when('/dashboard', {
	    templateUrl: '/secure/dashboard/dashboard.html',
	    controller: 'DashboardCtrl'
	  })
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