'use strict';

var app=angular.module('testerWebApp',['ngResource','base64','ngRoute']);

app.factory("User", function($resource) {
	return $resource("http://localhost:8080/user");
});

app.config(function($routeProvider, $locationProvider) {
	  $routeProvider
	   .when('/dashboard', {
	    templateUrl: '/secure/dashboard/dashboard.html',
	    controller: 'DashboardCtrl',
	  })
	  .when('/login', {
	    templateUrl: '/login/login.html',
	    controller: 'LoginCtrl',
	  })
});