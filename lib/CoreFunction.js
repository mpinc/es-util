var async = require('async');
function createIndexMapping(sClient,logger,index,typeName,mapping,callback){
    //create document type
    sClient.indices.putMapping({
        index:index,
        type : typeName,
        body: mapping
    },function(error,response){
        if (error) {
            callback(error);
        }else{
            callback(null,true);
        }});
}

function rotateIndex(sClient,logger,index1,index2,alias,callback){
    sClient.indices.existsAlias({index:index1,name:alias},function (error,result){
        if (error){
            callback(error);
        }
        if (result) {
            callback(null,{curIndex: index2,oldIndex:index1});
        }else{
            callback(null,{curIndex: index1,oldIndex:index2});
        }
    })
}

function deleteIndex(sClient,logger,index,callback){
    sClient.indices.exists({index:index},function(error, result){
        if (error){
            callback(error);
        }
        if (result){
            sClient.indices.delete({
                index: index
            }, function (error,response) {
                if (error) {
                    callback(error);
                }else{
                    callback(null,true);
                }
            });
        }else{
            callback(null,true);
        }
    })
}

function createIndex(sClient,logger,index, callback){
    //create product index
    sClient.indices.create({
        index: index
    }, function (error,response) {
        if (error) {
            callback(error);
        }else{
            callback(null,true);
        }
    });
}

function aliasIndex(sClient,logger,index1, index2,alias,callback){
    //rotate alias to this new index
    sClient.indices.deleteAlias(
        { index: index2, name: alias
        }, function (error,result){
            if (error) {
                logger.info("alias not exists [index:"+index2+" alias:"+alias+"]");
            }
            //rotate alias to this new index
            sClient.indices.putAlias(
                { index: index1, name: alias
                }, function (error,result){
                    if (error) {
                        callback(error);
                    }
                    else{
                        callback(null,true);
                    }
                })
        })
}

function indexData(sClient,logger,index,indexType,indexFunction,pageSize,callback){
    var start= 0,hasNext=true;
    async.doWhilst(function(cb) {
        indexFunction(sClient,index, indexType,start, pageSize, function (error, hasNextPage) {
            if (error) {
                return callback(error);
            }
            if (hasNextPage) {
                hasNext = true;
                start += pageSize;
                cb();
            } else {
                hasNext = false;
                cb();
            }
        });
    },function(){return hasNext},function(error,result){
        if (error) {
            return callback(error);
        } else {
            return callback(null,true);
        }
    });
}

function search(sClient,logger,index,indexType,searchBody,callback){
    sClient.search({
            index: index,
            type : indexType,
            body:  searchBody
        }
        , function (error,response) {
            if (error){
                callback(error);
            }else{
                callback(null,response?response.hits:null);
            }
        });
}
function searchAggregations(sClient,logger,index,indexType,searchBody,callback){
    sClient.search({
            index: index,
            type : indexType,
            body:  searchBody
        }
        , function (error,response) {
            if (error){
                callback(error);
            }else{
                callback(null,response?response:null);
            }
        });
}

module.exports = {
    createIndexMapping: createIndexMapping,
    aliasIndex: aliasIndex,
    createIndex: createIndex,
    deleteIndex:deleteIndex,
    rotateIndex:rotateIndex,
    createIndexMapping:createIndexMapping,
    indexData:indexData,
    search:search,
    searchAggregations:searchAggregations
};