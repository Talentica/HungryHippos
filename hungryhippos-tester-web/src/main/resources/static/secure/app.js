'use strict';

var app=angular.module('testerWebApp',['ngResource','ngRoute','ui.bootstrap','angular.filter','angularSpinner','angularFileUpload','720kb.tooltips']);

app.factory("JobHistoryResource", function($resource) {
	return $resource("/job/history/");
});

app.factory("UserResource", function($resource) {
	return $resource("/user");
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

app.directive("limitTo", [function() {
    return {
        restrict: "A",
        link: function(scope, elem, attrs) {
        	limitCharactersInInputText(scope,elem,attrs);
        }
    }
}]);

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

app.directive('fileSelect', function() {
	  var template = '<input type="file" name="files"/>';
	  return function( scope, elem, attrs ) {
	    var selector = $( template );
	    elem.append(selector);
	    selector.bind('change', function( event ) {
	      scope.$apply(function() {
	        scope[ attrs.fileSelect ] = event.originalEvent.target.files;
	      });
	    });
	    scope.$watch(attrs.fileSelect, function(file) {
	      selector.val(file);
	    });
	  };
	});

app.config(function($routeProvider, $locationProvider) {
	  $routeProvider
	  .when('/newjob', {
	    templateUrl: 'job/newjob.html',
	    controller: 'NewJobCtrl'
	  })
	  .when('/history', {
	    templateUrl: 'job/jobHistory.html',
	    controller: 'JobHistoryCtrl'
	  })
	  .when('/about', {
		    templateUrl: 'about.html',
		  })
	  .when('/faq', {
		    templateUrl: 'faq.html',
		  })
});