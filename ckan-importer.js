/*
 *  Import data from ckan server
 *  Think this should run as a child process
 */
var ckan = require('node-ckan');
var http = require('http');
var fs = require('fs');
var MongoClient = require('mongodb').MongoClient;

// stored id in mqe
var CKAN_ID = 'ckan_id';

var verbose = false;

// hash of known parsers, directory should be set in 
var parsers = {};
var itemProcessor = null;

var index = 0;
var packageList = [];
// list of all ckan id's in ckan system
var packageIdList = [];
var collection, mongoclient;

var stats = {};

// tags show w/ vocab id only, not name
// we will look up names and cache here
var vocabMap = {};

function run(config) {
    index = 0;
    resetStats();
    verbose = config.import.verbose;
    if( verbose ) console.log("Exporting data from CKAN...");

    setKnownParsers();
    setItemPostProcess();

    ckan.export({
        server   : config.ckan.server,
        debug : verbose,
        callback : function(data){

            for( var id in data.packages ) {
                packageIdList.push(id);
                packageList.push(data.packages[id]);
            }
            if( verbose ) console.log("Data Loaded. "+packageList.length+" pkgs found.");
            onDataReady(config);
        }
    });
}

function setKnownParsers() {
    parsers = {};
    if( !config.import.parsers ) return;

    var files = fs.readdirSync(config.import.parsers);
    for( var i = 0; i < files.length; i++ ) {
        if( !files[i].match(/^\..*/) ) {
            parsers[files[i].replace(/\.js$/,'')] = require(config.import.parsers+"/"+files[i]);
        }
    }
}

// different import process might want to muck with the item after its created,
// they can set a hook here to do this
function setItemPostProcess() {
    if( config.import.itemProcessor ) {
        itemProcessor = require(config.import.itemProcessor);
    }
}

function onDataReady(config, data) {
    if( verbose ) console.log("Connecting to mongo...");
    MongoClient.connect(config.db.url, function(err, database) {
        if( err ) {
            stats.errLog.push("Failed to connect to mongodb @ "+config.db.url);
            return console.log(err);
        }
        mongoclient = database;

        database.collection(config.db.mainCollection, function(err, coll) { 
            if( err ) {
                stats.errLog.push("Failed to connect to collection: "+config.db.mainCollection);
                return callback(err);
            }

            if( verbose ) console.log("Connected.  Updating resources...");
            collection = coll;
            updatePackage(packageList[0]);
        });
    });
}


function onPackageComplete() {
    index++;
    if( index == packageList.length ) {

        if( verbose ) console.log("Removing old records...");
        removeOldRecords(function(){

            if( verbose ) console.log("Clearing cache...");
            clearCache(function(){

                if( verbose ) console.log("Saving stats...");
                saveStats();
                if( verbose ) console.log("Done.");
            });
        });
    } else {
        updatePackage(packageList[index]);
    }
}


function updatePackage(pkg) {
    if( verbose ) console.log("\nUpdating packages("+index+"/"+packageList.length+"): "+pkg.title);
    if( pkg.resources.length == 0 ) {
        if( verbose ) console.log(" --Package has no resources.");
        onPackageComplete();
        return;
    } 

    if( config.ckan.groupByPackage ) {
        updateRecordGroup(pkg, function() {
            onPackageComplete();
        });

    } else {
        // for each resource package update records
        updateRecord(0, pkg, function(){
            onPackageComplete();
        });
    }
}

function updateRecordGroup(pkg, callback) {    
    createItemGroup(pkg, function(item){
        function next() {
            callback();
        }

        updateItem(item, next, callback);
    });
}

function updateRecord(rindex, pkg, callback) {
    var resource = pkg.resources[rindex];
    if( verbose ) console.log("Checking resource("+rindex+"/"+pkg.resources.length+"): "+resource.name);
    
    createItem(pkg, resource, function(item){
        function next() {
            rindex++;
            if( rindex == pkg.resources.length ) {
                callback();
            } else {
                updateRecord(rindex, pkg, callback);
            }
        }

        updateItem(item, next, callback);
    });    
}

function updateItem(item, next, callback) {
    function update(insert) {
        if( verbose ) {
            if( insert ) console.log(" --resource doesn't exist: inserting...");
            else console.log(" --resource is stale, updating...");
        }

        // stale, download file and parse data (if parser exsits)
        getData(item, function(){

            var action = "save";
            if( insert ) action = "insert";

            collection[action](item, {w :1}, function(err, result) {
                if( err ) {
                    stats.error++;
                    console.log(err);
                    stats.errLog.push("Failed to create ckan resource: "+item.ckan_id+". "+JSON.stringify(err));
                } else {
                    if( insert ) stats.inserted++;
                    else stats.updated++;

                    if( verbose ) console.log(" --update complete");
                }
                next();
            });
        });
    }

    // first see if the record exits
    collection.find({ckan_id: item[CKAN_ID]}).toArray(function(err, items) {
        if( err ) {
            stats.error++;
            console.log(err);
            stats.errLog.push("Failed to find ckan resource: "+item.ckan_id+". "+JSON.stringify(err));
            return next();
        }

        if( items.length > 0 ) { // exists
            item._id = items[0]._id;

            if( itemChanged(item, items[0]) ) {
                update();
            } else {
                next();
                stats.syncd++;

                if( verbose ) console.log("  --resources are in sync.");
            }
        } else {
            update(true);
        }

    });
}


// if we have a parser for the format, download the data and parse
function getData(item, callback) {
    if( !parsers[item.format] || !item.url ) return callback();

    var data = "";
    http.get(item.url, function(response) {
        response.setEncoding('utf-8');

        response.addListener('data', function (chunk) {
            data += chunk;
        });
        response.addListener("end", function() {
            var file = parsers[item.format].parse(data);
            for( var key in file.filters ) item[key] = file.filters[key];
            item.data = file.data;
            callback();
        });
    });
}

// compare to items and see if anything has changed
function itemChanged(r1, r2) {
    for( var key in r1 ) {

        // TODO: this should be a black list
        if( key == "data" ) continue;

        if( typeof r1[key] == "string" ) {
            if( r1[key] != r2[key] ) return true;
        } else {
            if( !r2[key] ) return true;
            if( r1[key].length != r2[key].length ) return true;
            for( var i = 0; i < r1[key].length; i++ ) {
                if( typeof r1[key][i] != typeof r2[key][i] ) return true;

                if( typeof r1[key][i] == 'object' ) {
                    if( JSON.stringify(r1[key][i]) != JSON.stringify(r2[key][i]) ) return true;
                } else {
                    if( r1[key][i] != r2[key][i] ) return true;
                }

                
            }
        }
    }
    return false;
}


// create a mqe item from a pkg and a resource
function createItem(pkg, res, callback){
    var item = {
        title        : res.name,
        description  : res.description,
        created      : res.created,
        url          : res.url,
        format       : res.format,
        ckan_id      : res.id,
        updated      : res.revision_timestamp,
        groups       : [],
        organization : pkg.organization ? pkg.organization.title : "",
        package      : pkg.title,
        notes        : pkg.notes
    }

    // add extras
    if( pkg.extras ) {
        for( var i = 0; i < pkg.extras.length; i++ ) {
            if( pkg.extras[i].state == 'active' ) {
                item.extras[pkg.extras[i].key] = pkg.extras[i].value;
            }
        }
    }

    if( pkg.groups ) {
        for( var i = 0; i < pkg.groups.length; i++ ) {
            item.groups.push(pkg.groups[i].name);
        }
    }

    setTags(item, pkg, callback);
}

// create a mqe item from a pkg and a resource
function createItemGroup(pkg, callback){
    var item = {
        title        : pkg.title,
        description  : pkg.notes,
        created      : pkg.metadata_created,
        url          : config.ckan.server+"/dataset/"+pkg.name,
        ckan_id      : pkg.id,
        updated      : pkg.revision_timestamp,
        groups       : [],
        resources    : pkg.resources ? pkg.resources : [],
        organization : pkg.organization ? pkg.organization.title : "",
        extras       : {}
    }

    // add extras
    if( pkg.extras ) {
        for( var i = 0; i < pkg.extras.length; i++ ) {
            if( pkg.extras[i].state == 'active' ) {
                item.extras[pkg.extras[i].key] = pkg.extras[i].value;
            }
        }
    }
    

    if( pkg.groups ) {
        for( var i = 0; i < pkg.groups.length; i++ ) {
            item.groups.push(pkg.groups[i].name);
        }
    }

    setTags(item, pkg, callback);
}

function setTags(item, pkg, callback) {
    if( !pkg.tags ) {
        if( itemProcessor ) itemProcessor.process(item);
        return callback(item);
    }

    setTag(0, item, pkg, callback);
}

function setTag(index, item, pkg, callback) {
    if( index == pkg.tags.length ) {
        if( itemProcessor ) itemProcessor.process(item);
        return callback(item);
    }

    var tag = pkg.tags[index];
    if( tag.state != "active" ) {
        index++;
        setTag(index, item, pkg, callback);
    }

    if( !tag.vocabulary_id ) {
        if( !item.tags ) item.tags = [];

        item.tags.push(tag.display_name);
        index++;
        setTag(index, item, pkg, callback);
    } else if ( vocabMap[tag.vocabulary_id] ) {
        if( !item[vocabMap[tag.vocabulary_id]] ) item[vocabMap[tag.vocabulary_id]] = [];

        item[vocabMap[tag.vocabulary_id]].push(tag.display_name);
        index++;
        setTag(index, item, pkg, callback);
    } else {
        ckan.exec("vocabulary_show", {id:tag.vocabulary_id}, function(err, resp){
            if( err && verbose ) {
                console.log("Unable to lookup vocabulary_id");
            } else {
                vocabMap[tag.vocabulary_id] = resp.result.name;
                if( !item[resp.result.name] ) item[resp.result.name] = [];

                item[resp.result.name].push(tag.display_name);
                index++;
                setTag(index, item, pkg, callback);
            }

        });
    }

}

function resetStats() {
    stats = {
        syncd    : 0,
        updated  : 0,
        inserted : 0,
        removed  : 0,
        // TODO: push messages to error log when things aren't working
        errors   : 0,
        errLog   : [] 
    }
}


function removeOldRecords(callback) {
    if( stats.updated == 0 && stats.inserted == 0 ) return callback();

    // grab all current record from the db
    collection.find({},{ckan_id:1}).toArray(function(err, items) {
        if( err && verbose ) {
            console.log("Failed to load records to check for deletion");
        }
        if( err ) return callback();

        var removeList = [];
        for( var i = 0; i < packageIdList.length; i++ ) {
            var found = false;
            for( var j = 0; j < items.length; j++ ) {
                if( packageIdList[i] == items[j].ckan_id ) {
                    found = true;
                    break;
                }
            }
            if( !found ) removeList.push(items[i].ckan_id);
        }

        collection.remove({'ckan_id' : { $in : removeList } },function(err, removed){
            if( err && verbose ) console.log("Failed to remove old records");
            stats.removed = removeList.length;
            callback();
        });
    });
}

// clear the mqe cache if inserts or update > 1 and log if errors if fail
function clearCache(callback) {
    if( stats.updated == 0 && stats.inserted == 0 && stats.removed == 0 ) return callback();

    mongoclient.collection(config.db.cacheCollection, function(err, collection) { 
        if( err ) console.log(err);
        collection.remove({},function(err, removed){
            if( err ) stats.errLog.push("Failed to clear cache: "+JSON.stringify(err));
            callback();
        });
    });
}

// this should never block anything
function saveStats() {
    stats.timestamp = new Date().getTime();

    if( verbose ) console.log(stats);

    if( config.import.statsCollection ) {
        mongoclient.collection(config.import.statsCollection, function(err, collection) { 
            collection.insert(stats,function(err, removed){
                mongoclient.close();
            });
        });
    } else {
        mongoclient.close();
    }
    
}


// the config file should be the second argument
if( process.argv.length < 3 ) {
    console.log("you must provide the location of your config file");
    process.exit();
}

config = require(process.argv[2]);
run(config);