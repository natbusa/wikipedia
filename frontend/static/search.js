function SearchCtrl($scope, $http, $log) {
  $scope.searchResults = [];

  $scope.fetchPages = function(data, status) {
   angular.forEach(data, function(value, key) {
      $log.log(value)
      $scope.searchResults.push(value[0])
    })
  }

  $scope.search = function() {
    $scope.searchResults = [];
    $http.get('/word/'+$scope.searchText).success($scope.fetchPages);
  };
}


