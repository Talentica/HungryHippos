'use strict';

var app=angular.module('testerWebApp',['ngResource','base64','ngRoute']);

app.factory("User", function($resource) {
	return $resource("/user");
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
	    templateUrl: 'secure/newjob/newjob.html',
	    controller: 'NewJobCtrl'
	  })
});