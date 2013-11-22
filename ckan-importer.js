/*
 *	Import data from ckan server
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

var index = 0;
var packageList = [];
var collection, mongoclient;

var stats = {};

function run(config) {
	index = 0;
	resetStats();
	if( verbose ) console.log("Exporting data from CKAN...");

	setKnownParsers();

	ckan.export({
		server   : config.ckan.server,
		callback : function(data){
			for( var id in data.packages ) {
				packageList.push(data.packages[id]);
			}
			if( verbose ) console.log("Data Loaded. "+packageList.length+" pkgs found.");
			onDataReady(config);
		}
	});
}

function setKnownParsers() {
	parsers = {};
	if( !config.ckan.parsers ) return;

	var files = fs.readdirSync(config.ckan.parsers);
	for( var i = 0; i < files.length; i++ ) {
		if( !file[i].match(/^\..*/) && file[i].match(/^\..*/) ) {
			parsers[file[i].replace(/\.js$/,'')] = require(config.ckan.parsers+"/"+files[i]);
		}
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
		if( verbose ) console.log("Saving stats...");
		saveStats();
		if( verbose ) console.log("Done.");
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

	// for each package update records
	updateRecord(0, pkg, function(){
		onPackageComplete();
	});
}

function updateRecord(rindex, pkg, callback) {
	var resource = pkg.resources[rindex];
	if( verbose ) console.log("Checking resource("+rindex+"/"+pkg.resources.length+"): "+resource.name);
	var item = createItem(pkg, resource);

	function next() {
		rindex++;
		if( rindex == pkg.resources.length ) {
			callback();
		} else {
			updateRecord(rindex, pkg, callback);
		}
	}

	function update(insert) {
		if( verbose ) {
			if( insert ) console.log(" --resource doesn't exist: inserting...");
			else console.log(" --resource is stale, updating...");
		}

		// stale, download file and parse data (if parser exsits)
		getData(item, function(){
			collection.insert(item, {w :1}, function(err, result) {
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
			if( resourceChanged(item, items[0]) ) {
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
function resourceChanged(r1, r2) {
	for( var key in r1 ) {
		if( key == "data" ) continue;

		if( typeof r1[key] == "string" ) {
			if( r1[key] != r2[key] ) return true;
		} else {
			if( !r2[key] ) return true;
			if( r1[key].length != r2[key].length ) return true;
			for( var i = 0; i < r1[key].length; i++ ) {
				if( r1[key][i] != r2[key][i] ) return true;
			}
		}
	}
	return false;
}


// create a mqe item from a pkg and a resource
function createItem(pkg, res){
	var item = {
		title        : res.name,
		description  : res.description,
		created      : res.created,
		url          : res.url,
		format       : res.format,
		ckan_id      : res.id,
		updated      : res.revision_timestamp,
		keywords     : [],
		groups       : [],
		organization : pkg.organization ? pkg.organization.title : "",
		package      : pkg.title,
		notes        : pkg.notes
	}

	if( pkg.groups ) {
		for( var i = 0; i < pkg.groups.length; i++ ) {
			item.groups.push(pkg.groups[i].name);
		}
	}

	if( pkg.tags ) {
		for( var i = 0; i < pkg.tags.length; i++ ) {
			if( pkg.tags[i].state == "active" ) {
				item.keywords.push(pkg.tags[i].display_name);
			}
		}
	}

	return item;
}

function resetStats() {
	stats = {
		syncd    : 0,
		updated  : 0,
		inserted : 0,
		// TODO: push messages to error log when things aren't working
		errors   : 0,
		errLog   : [] 
	}
}

// this should never block anything
function saveStats() {
	stats.timestamp = new Date().getTime();

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