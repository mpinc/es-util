var cf = require('./CoreFunction.js');
var searchClient = require('./SearchClient.js');
var Seq = require('seq');

function ESUtil(config,logger){
    this.config=config
    this.sClient=searchClient.getSearchClient(config);
    this.logger=logger;

    this.doRotateIndex=function(indexType,indexMapping,alias,index1, index2,indexFunction, callback){
        var curIndex,oldIndex;

        var prodList  = [];
        Seq().seq(function() {
            var that = this;
            cf.rotateIndex(index1,index2,alias,function(error,result){
                if (error) {
                    logger.error('rotate index:' + error.message);
                    return callback(error);
                }
                curIndex=result.curIndex;
                oldIndex=result.oldIndex;
                that();
            })
        }).seq(function(){
            //Delete this index
            var that = this;
            cf.deleteIndex(curIndex,function(error,result){
                if (error){
                    logger.error('delete index:' +curIndex+":"+ error.message);
                    return callback(error);
                }else{
                    that();
                }
            })

        }).seq(function(){
            var that = this;
            cf.createIndex(curIndex,function(error,result){
                if (error){
                    logger.error('create index:' +curIndex+":"+ error.message);
                    return callback(error);
                }else{
                    that();
                }
            })
        }).seq(function(){
            var that = this;
            cf.createIndexMapping(curIndex,indexType,indexMapping,function(error,response){
                if (error) {
                    logger.error(' create index type Mapping ' + error.message);
                    return callback(error);
                }else{
                    //logger.info(response);
                    that();
                }
            })
        }).seq(function(){
            var that = this;
            cf.indexData(tenant,curIndex,indexType,indexFunction,function(error,response){
                if (error) {
                    logger.error('index data ' + error.message);
                    return callback(error);
                }else{
                    //logger.info(response);
                    that();
                }
            })
        }).seq(function() {
            cf.aliasIndex(curIndex, oldIndex, alias, function (error, response) {
                    if (error) {
                        logger.error('alias index ' + error.message);
                        return callback(error);
                    }else{
                        return callback();
                    }
                }
            )
        });
    }
}

exports.ESUtil = ESUtil;