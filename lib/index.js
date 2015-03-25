var _ = require('lodash'),
    BPromise = require('bluebird'),
    async = require('async'),
    http = require('http'),
    inquirer = require('inquirer'),
    elasticsearch = require('elasticsearch'),
    client = new elasticsearch.Client({
        host: 'http://localhost:9200',
        requestTimeout: 180000
        //log: 'trace'
    }),
    apiKey = 'fa979e0be7fdb14074c153297a15f4fd';
inquirer.prompt([
    {
        type: 'checkbox',
        message: 'Select actions',
        name: 'actions',
        choices: [
            new inquirer.Separator('Elastic Search:'),
            {
                name: 'Leave Existing Data & Structure'
            },
            {
                name: 'Delete & Recreate Indexes',
                checked: true
            },
            new inquirer.Separator('Indexes:'),
            {
                name: 'Category',
                checked: true
            },
            {
                name: 'Style',
                checked: true
            },
            {
                name: 'Brewery SAMPLE',
                checked: true
            },
            {
                name: 'Beer SAMPLE',
                checked: true
            },
            {
                name: 'Location SAMPLE',
                checked: true
            }
            /*,
            new inquirer.Separator('Full data sets:'),
            {
                name: 'Brewery (~6100)'
            },
            {
                name: 'Beer (~38,000+)'
            }*/
        ],
        validate: function (answer) {
            if (answer.length < 1) {
                return 'You must choose at least one option.';
            }
            return true;
        }
    }
], function (answers) {
    deleteIndexes(answers.actions).then(function () {
        doWork(answers.actions);
    });

});


function doWork(actions) {


    var categoryPromise, stylePromise, brewerySamplePromise, beerSamplePromise, locationSamplePromise, doActions;

    categoryPromise = new BPromise(function (resolve) {
        loadCategoryData(resolve);
        //resolve();
    });

    stylePromise = new BPromise(function (resolve) {
        loadStyleData(resolve);
        //resolve();
    });

    brewerySamplePromise = new BPromise(function (resolve) {
        loadBreweryData(resolve, 1, 3);
        //resolve();
    });

    beerSamplePromise = new BPromise(function (resolve) {
        loadBeerData(resolve, 1, 3);
        //resolve();
    });

    locationSamplePromise = new BPromise(function (resolve) {
        loadLocationData(resolve, 1, 3);
        //resolve();
    });

    doActions = [];

    _.each(actions, function (action) {
        switch (action) {
            case 'Category':
                doActions.push(categoryPromise);
                break;
            case 'Style':
                doActions.push(stylePromise);
                break;
            case 'Brewery SAMPLE':
                doActions.push(brewerySamplePromise);
                break;
            case 'Beer SAMPLE':
                doActions.push(beerSamplePromise);
                break;
            case 'Location SAMPLE':
                doActions.push(locationSamplePromise);
                break;
        }
    });

    console.log(doActions);
    return BPromise.all(doActions).then(function () {
        console.log('all done');
        process.exit(0);
    });
}

function deleteIndexes(actions) {
    console.log(JSON.stringify(actions));
    var deleteCategoryPromise, deleteBeerPromise, deleteLocationPromise, deleteBreweryPromise, deleteIndexes;
    if (_.contains(actions, 'Delete & Recreate Indexes')) {
        deleteIndexes = [deleteCategoryPromise, deleteBeerPromise, deleteLocationPromise, deleteBreweryPromise];
        console.log('here');
    } else {
        console.log('not here');
        deleteIndexes = [];
    }
    deleteCategoryPromise = new BPromise(function (resolve) {
        client.indices.delete({
            index: 'category'
        }, function (error, response) {
            console.log(error);
            console.log(JSON.stringify(response));
            resolve();
        });
    });
    deleteBeerPromise = new BPromise(function (resolve) {
        client.indices.delete({
            index: 'beer'
        }, function (error, response) {
            console.log(error);
            console.log(JSON.stringify(response));
            resolve();
        });
    });
    deleteLocationPromise = new BPromise(function (resolve) {
        client.indices.delete({
            index: 'location'
        }, function (error, response) {
            console.log(error);
            console.log(JSON.stringify(response));
            resolve();
        });
    });
    deleteBreweryPromise = new BPromise(function (resolve) {
        client.indices.delete({
            index: 'brewery'
        }, function (error, response) {
            console.log(error);
            console.log(JSON.stringify(response));
            resolve();
        });
    });

    return BPromise.all(deleteIndexes);

};

function loadStyleData(resolve) {

    var options = {
        host: 'api.brewerydb.com',
        port: '80',
        path: '/v2/styles?key=' + apiKey,
        method: 'GET'
    };

    http.request(options, function (response) {
        var str = '';
        response.on('data', function (chunk) {
            str += chunk;
        });
        response.on('end', function () {
            var res = JSON.parse(str);
            //console.log(res);
            console.log('loading styles ');

            async.each(res.data,
                function (item, callback) {
                    client.create({
                        index: 'category',
                        type: 'styles',
                        id: item.id,
                        body: item
                    }, function (error, response) {
                        if (error) {
                            console.log(error);
                        }
                        callback();
                    });
                },
                function (err) {
                    console.log('styles done');
                    resolve();
                }
            );
        });
    }).end();
};

function loadCategoryData(resolve) {

    var options = {
        host: 'api.brewerydb.com',
        port: '80',
        path: '/v2/categories?key=' + apiKey,
        method: 'GET'
    };

    http.request(options, function (response) {
        var str = '';
        response.on('data', function (chunk) {
            str += chunk;
        });
        response.on('end', function () {
            var res = JSON.parse(str);
            //console.log(res);
            console.log('loading categories ');

            async.each(res.data,
                function (item, callback) {
                    client.create({
                        index: 'category',
                        type: 'categories',
                        id: item.id,
                        body: item
                    }, function (error, response) {
                        if (error) {
                            console.log(error);
                        }
                        callback();
                    });
                },
                function (err) {
                    console.log('categories done');
                    resolve();
                }
            );
        });
    }).end();
};

function loadBeerData(resolve, currentPage, maxPages) {
    console.log('getting beer page:' + currentPage);
    var numberOfPages = currentPage,
        options = {
            host: 'api.brewerydb.com',
            port: '80',
            path: '/v2/beers?key=' + apiKey + '&p=' + currentPage + '&withBreweries=Y',
            method: 'GET'
        };

    http.request(options, function (response) {
        var str = '';
        response.on('data', function (chunk) {
            str += chunk;
        });
        response.on('end', function () {
            var res = JSON.parse(str);

            //console.log(res);
            numberOfPages = res.numberOfPages;
            console.log('got beer page:' + currentPage + ' of ' + numberOfPages);
            //console.log(res.data.length);
            //console.log(res.data);

            async.eachSeries(res.data,
                function (item, callback) {
                    //console.log('item::' + JSON.stringify(item));
                    //clean up breweries information
                    var breweries = [], x, z;
                    if (item.breweries) {
                        for (x = 0, z = item.breweries.length; x < z; x++) {
                            breweries.push({id: item.breweries[x].id, name: item.breweries[x].name});
                        }
                    }
                    delete item.breweries;
                    item.breweries = breweries;
                    //end brewery clean up

                    //clean up category & style information.
                    item.styleName = item.style ? item.style.name : '';
                    item.categoryId = item.style ? item.style.categoryId : 0;
                    item.categoryName = item.style ? item.style.category.name : '';
                    delete item.style;
                    //end style clean up


                    client.create({
                        index: 'beer',
                        type: item.styleId ? item.styleId : 0,
                        id: item.id,
                        body: item
                    }, function (error, response) {
                        if (error) {
                            console.log(error);
                        }
                        callback();
                    });

                },
                function (err) {
                    currentPage += 1;

                    if (currentPage <= numberOfPages && currentPage <= maxPages) {
                        loadBeerData(resolve, currentPage, maxPages);
                    } else {
                        console.log('beer done');
                        resolve();
                    }
                }
            );
        });
    }).end();
};

function loadBreweryData(resolve, currentPage, maxPages) {
    console.log('getting brewery page:' + currentPage);
    var numberOfPages = currentPage,
        options = {
            host: 'api.brewerydb.com',
            port: '80',
            path: '/v2/breweries?key=' + apiKey + '&p=' + currentPage,
            method: 'GET'
        };

    http.request(options, function (response) {
        var str = '';
        response.on('data', function (chunk) {
            str += chunk;
        });
        response.on('end', function () {
            var res = JSON.parse(str);
            numberOfPages = res.numberOfPages;
            console.log('got brewery page:' + currentPage + ' of ' + numberOfPages);

            async.eachSeries(res.data,
                function (item, callback) {
                    client.create({
                        index: 'brewery',
                        type: 'breweries',
                        id: item.id,
                        body: item
                    }, function (error, response) {
                        if (error) {
                            console.log(error);
                        }
                        callback();
                    });

                },
                function (err) {
                    currentPage += 1;

                    if (currentPage <= numberOfPages && currentPage <= maxPages) {
                        loadBreweryData(resolve, currentPage, maxPages);
                    } else {
                        console.log('brewery done');
                        resolve();
                    }
                }
            );
        });
    }).end();
};

function loadLocationData(resolve, currentPage, maxPages) {
    console.log('getting location page:' + currentPage);
    var numberOfPages = currentPage,
        options = {
            host: 'api.brewerydb.com',
            port: '80',
            path: '/v2/locations?key=' + apiKey + '&p=' + currentPage,
            method: 'GET'
        };

    http.request(options, function (response) {
        var str = '';
        response.on('data', function (chunk) {
            str += chunk;
        });
        response.on('end', function () {
            var res = JSON.parse(str);
            numberOfPages = res.numberOfPages;
            console.log('got location page:' + currentPage + ' of ' + numberOfPages);

            async.eachSeries(res.data,
                function (item, callback) {
                    client.create({
                        index: 'location',
                        type: item.locationType,
                        id: item.id,
                        body: item
                    }, function (error, response) {
                        if (error) {
                            console.log(error);
                        }
                        callback();
                    });

                },
                function (err) {
                    currentPage += 1;

                    if (currentPage <= numberOfPages && currentPage <= maxPages) {
                        loadLocationData(resolve, currentPage, maxPages);
                    } else {
                        console.log('location done');
                        resolve();
                    }
                }
            );
        });
    }).end();
};