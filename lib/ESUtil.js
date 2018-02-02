var cf = require('./CoreFunction.js');
var searchClient = require('./SearchClient.js');
var Seq = require('seq');
var extend = require('extend');

function ESUtil(aConfig,aLogger){
    //clone an elasticsearch config
    this.config={};
    extend(this.config,aConfig);
    this.sClient=searchClient.getSearchClient(this.config);
    this.logger=aLogger;

    this.doRotateIndex=function(indexType,indexMapping,alias,index1, index2,indexFunction, pageSize,callback){
        var curIndex,oldIndex;
        var sClient=this.sClient;
        var logger=this.logger;

        var prodList  = [];
        Seq().seq(function() {
            var that = this;
            cf.rotateIndex(sClient,logger,index1,index2,alias,function(error,result){
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
            cf.deleteIndex(sClient,logger,curIndex,function(error,result){
                if (error){
                    logger.error('delete index:' +curIndex+":"+ error.message);
                    return callback(error);
                }else{
                    that();
                }
            })

        }).seq(function(){
            var that = this;
            cf.createIndex(sClient,logger,curIndex,function(error,result){
                if (error){
                    logger.error('create index:' +curIndex+":"+ error.message);
                    return callback(error);
                }else{
                    that();
                }
            })
        }).seq(function(){
            var that = this;
            cf.createIndexMapping(sClient,logger,curIndex,indexType,indexMapping,function(error,response){
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
            cf.indexData(sClient,logger,curIndex,indexType,indexFunction,pageSize,function(error,response){
                if (error) {
                    logger.error('index data ' + error.message);
                    return callback(error);
                }else{
                    //logger.info(response);
                    that();
                }
            })
        }).seq(function() {
            cf.aliasIndex(sClient,logger,curIndex, oldIndex, alias, function (error, response) {
                    if (error) {
                        logger.error('alias index ' + error.message);
                        return callback(error);
                    }else{
                        return callback();
                    }
                }
            )
        });
    };

    this.search=function(index,indexType,searchBody,callback){
        var sClient=this.sClient;
        var logger=this.logger;
        cf.search(sClient,logger,index,indexType,searchBody,callback);
    };
    this.searchAll=function(index,indexType,searchBody,callback){
        var sClient=this.sClient;
        var logger=this.logger;
        cf.searchAll(sClient,logger,index,indexType,searchBody,callback);
    }
}

exports.ESUtil = ESUtil;