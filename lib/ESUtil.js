var cf = require('./CoreFunction.js');
var searchClient = require('./SearchClient.js');
var Seq = require('seq');
var extend = require('extend');

function ESUtil(aConfig,aLogger){
    //clone an elasticsearch config
    extend(this.config,aConfig);
    this.sClient=searchClient.getSearchClient(this.config);
    this.logger=aLogger;

    var that = this;

    this.doRotateIndex=function(indexType,indexMapping,alias,index1, index2,indexFunction, pageSize,callback){
        var curIndex,oldIndex;

        var prodList  = [];
        Seq().seq(function() {
            var that = this;
            cf.rotateIndex(that.sClient,index1,index2,alias,function(error,result){
                if (error) {
                    that.logger.error('rotate index:' + error.message);
                    return callback(error);
                }
                curIndex=result.curIndex;
                oldIndex=result.oldIndex;
                that();
            })
        }).seq(function(){
            //Delete this index
            var that = this;
            cf.deleteIndex(that.sClient,curIndex,function(error,result){
                if (error){
                    that.logger.error('delete index:' +curIndex+":"+ error.message);
                    return callback(error);
                }else{
                    that();
                }
            })

        }).seq(function(){
            var that = this;
            cf.createIndex(that.sClient,curIndex,function(error,result){
                if (error){
                    that.logger.error('create index:' +curIndex+":"+ error.message);
                    return callback(error);
                }else{
                    that();
                }
            })
        }).seq(function(){
            var that = this;
            cf.createIndexMapping(that.sClient,curIndex,indexType,indexMapping,function(error,response){
                if (error) {
                    that.logger.error(' create index type Mapping ' + error.message);
                    return callback(error);
                }else{
                    //logger.info(response);
                    that();
                }
            })
        }).seq(function(){
            var that = this;
            cf.indexData(that.sClient,curIndex,indexType,indexFunction,pageSize,function(error,response){
                if (error) {
                    that.logger.error('index data ' + error.message);
                    return callback(error);
                }else{
                    //logger.info(response);
                    that();
                }
            })
        }).seq(function() {
            cf.aliasIndex(that.sClient,curIndex, oldIndex, alias, function (error, response) {
                    if (error) {
                        that.logger.error('alias index ' + error.message);
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