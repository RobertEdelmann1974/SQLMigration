/**
 * @type {plugins.http.HttpClientConfig}
 * @properties={typeid:35,uuid:"AE174274-D1D9-4164-8363-C2BAB057B274",variableType:-4}
 */
var httpConfig = null;

/**
 * @properties={typeid:24,uuid:"C7EE3256-0DFC-4C03-8EFE-7A643395AA16"}
 */
function createHttpConfig() {
	httpConfig = plugins.http.createNewHttpClientConfig();
	httpConfig.forceHttp1 = true;
}


/**
 * @properties={typeid:24,uuid:"78FDE048-D95E-46AF-8F0F-A321FD10B2B5"}
 */
function transferDB() {
	if (!scopes.settings.sqlPipeURL) {
		return;
	}
	createHttpConfig();
	var listDatabases = databaseManager.getServerNames();
 	for (var iDBs = 0; iDBs < listDatabases.length; iDBs++) {
		if (listDatabases[iDBs].toLocaleLowerCase() === 'bauprocheck' || listDatabases[iDBs].toLocaleLowerCase() === '_sv_inmem') {
			application.output('skipped: ' + listDatabases[iDBs]);
			continue;
		}
		var infoDB = 'Database: ' + listDatabases[iDBs] + ' [' + (iDBs + 1).toString() + ' / ' + listDatabases.length.toString() + '] ';
		var listTables = databaseManager.getTableNames(listDatabases[iDBs]);
		for (var iTables = 0; iTables < listTables.length; iTables++) {
			var infoTable = '- Table ' + listTables[iTables] + ' [' + (iTables + 1).toString() + ' / ' + listTables.length.toString() + ']';
			if (listDatabases[iDBs].toLocaleLowerCase().startsWith('bauprocheck') || listDatabases[iDBs].toLocaleLowerCase().startsWith('svy_framework')) {
				var requestObject = getTransferObject(listDatabases[iDBs],listTables[iTables]);
				var tries = 1
				do {
					var transfersQueued = checkTransfers('queued')
					application.sleep(500);
					var transfersRunning = checkTransfers('running')
					application.sleep(500);
					if (transfersQueued == null || !transfersRunning == null) {
						// keine Verbindung, Abbruch
						return;
					}
					if (transfersQueued + transfersRunning < 10) {
						if (sendTransferRequest(requestObject)) {
							application.output('Erfolg - ' + infoDB + infoTable);
							application.sleep(1000);
							break;
						} else {
							application.output('Problem - ' + infoDB + infoTable);
							// Abbruch
							break;
						}
					} else if (tries < 10) {
						application.sleep(3*1000);
						tries++;
					} else {
						break;
					}
				} while (true);
			}
		}
	}
}

/**
 * Objekt für die Übergabe der Daten
 *
 * @param {String} databaseName
 * @param {String} tableName
 * @return {Object}
 * @properties={typeid:24,uuid:"018104D7-E1F4-40C5-BA49-A9A599A0CF43"}
 */
function getTransferObject(databaseName, tableName) {
	if (databaseName == 'svy_framework') {
		databaseName = 'bauprocheck_framework';
	}
	var infoObject = {
		targetUserName: scopes.settings.target_user_name,
		databaseName: databaseName,
		tableName: tableName
	}
	return {
		  "source-name": scopes.settings.sourceName,
		  "source-type": scopes.settings.sourceType,
		  "source-connection-string": scopes.settings.sourceConnection + databaseName,
		  "target-name": scopes.settings.targetName,
		  "target-type": scopes.settings.targetType,
		  "target-connection-string": utils.stringReplaceTags(scopes.settings.targetConnectionString,infoObject),
		  "source-schema": scopes.settings.sourceSchema,
		  "source-table": tableName,
		  "target-table": tableName,
		  "drop-target-table-if-exists": true,
		  "create-target-table-if-not-exists": true,
		  "create-target-schema-if-not-exists": true,
		  "target-schema": scopes.settings.targetSchema,
		  "target-database": databaseName,
		  "target-password": scopes.settings.tagetPassword,
		  "target-hostname": scopes.settings.targetHostName,
		  "target-username": scopes.settings.targetUserName
		}
}

/**
 * @param {Object} requestObject
 * @return {Boolean}
 * @properties={typeid:24,uuid:"8C8329D7-C538-4A65-A2CD-63D6E9FA075F"}
 */
function sendTransferRequest(requestObject) {
	var httpClient = plugins.http.createNewHttpClient(httpConfig);
	var url = scopes.settings.sqlPipeURL + '/transfers/create';
	var request = httpClient.createPostRequest(url);
	request.setBodyContent(JSON.stringify(requestObject));
	var response = request.executeRequest();
	httpClient.close();
	var statusCode = response.getStatusCode();
	if (statusCode < 200 || statusCode >= 300) {
		application.output('error starting transfer: ' + statusCode + ' - ' + response.getResponseBody());
		return false;
	}
	return true;
}

/**
 * return number of processes of a specific type
 * valid types are: 'queued', 'running', 'complete', 'cancelled', 'error'
 *
 * @param {String} [type] type of transfer; can be 'queued', 'running', 'complete', 'cancelled', 'error'
 * @return {Number}
 * @properties={typeid:24,uuid:"98B2A22F-A274-4796-B068-B7040D726967"}
 */
function checkTransfers(type) {
	if (['queued', 'running', 'complete', 'cancelled', 'error'].indexOf(type) == -1) {
		type = null;
	}
	var httpClient = plugins.http.createNewHttpClient(httpConfig);
	var url = scopes.settings.sqlPipeURL + '/transfers/list';
	if (type) {
		url = url + '/?status=' + type;
	}
	var request = httpClient.createGetRequest(url);
	var response = request.executeRequest();
	httpClient.close();
	var statusCode = response.getStatusCode();
	if (statusCode < 200 || statusCode >= 300) {
		return null;
	}
	var responseObject = JSON.parse(response.getResponseBody());
	if (responseObject && responseObject.hasOwnProperty('transfers')) {
		var transfers = responseObject['transfers'];
		return Object.entries(transfers).length;
	}
	return null;
}