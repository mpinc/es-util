var elasticsearch = require('elasticsearch');

function getSearchClient(config){
    var searchClient = new elasticsearch.Client(config);
    return searchClient;
}

module.exports = {
    getSearchClient: getSearchClient
};