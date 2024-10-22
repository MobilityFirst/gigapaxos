// src/index.js

class InetSocketAddress {
    constructor(hostname, address, port) {
        this.hostname = hostname
        this.port = port;
        this.address = address;
        this.url = `http:\/\/${this.address}:${this.port}/`;
      }
}

var rc1 = new InetSocketAddress(null, "localhost", 3300);
var rc2 = new InetSocketAddress(null, "localhost", 3301);
//var rc3 = new InetSocketAddress(null, "localhost", 3301);
var appName = "";
var reconfigurators = [rc1, rc2];
var activeReplicaURLs;
var arUrlToIsaMap = new Map();
var latenciesMap = new Map();
var bestAR = null;

const CHECK_ACTIVE_ARs_DURATION = 30*1000; //30 seconds
const NO_ACTIVE_REPLICAS = "No active replicas available, please try after 30 seconds";
function successRequestMsg(qid){
    return `The request was successfully executed with QID = ${qid}`;
}
var defaultTimeout = 5000;
let intervalId = null;

async function measureLatency(activeReplicaUrl){
    const startTime = performance.now();
    try {
        const response = await fetch(activeReplicaUrl, {signal: AbortSignal.timeout(25000)});
        return performance.now() - startTime;
    } catch (error) {
        console.error("Error fetching data: ", error);
        return Number.MAX_SAFE_INTEGER;
    }
}

function getARWithLeastLatency(latenciesMap){
    return [...latenciesMap.entries()].reduce((a, e) => e[1] < a[1] ? e : a)[0];
}
async function periodicallyCheckActiveARs() {
    const randomRcIdx = Math.floor(Math.random() * 2) + 1;
    var rc = reconfigurators[randomRcIdx-1];
    await fetch(`${rc.url}?type=REQ_ACTIVES&name=${appName}`, {method: "GET"})
    .then((resp) => resp.json())
    .then((resp) => {
        activeReplicaURLs = resp["ACTIVE_REPLICAS"];
    })
    latenciesMap = new Map();
    for(let i=0;i<activeReplicaURLs.length;i++){
        let comps = activeReplicaURLs[i].split(":");
        const offsetURL =  `http:\/\/${comps[0].substring(1)}:${parseInt(comps[1]) + 300}/`;
        let latency = await measureLatency(offsetURL);
        latenciesMap.set(offsetURL, latency);
        arUrlToIsaMap.set(offsetURL, new InetSocketAddress(null, comps[0].substring(1),parseInt(comps[1]) + 300));
    }
    console.log("Current latencies :: ", latenciesMap);
    bestAR = getARWithLeastLatency(latenciesMap);
    console.log("Best latency is for AR :: ", bestAR);
}
function removeARAndChooseNextBestReplica(){
    latenciesMap.delete(bestAR);
    return getARWithLeastLatency(latenciesMap);
}


async function sendRequest(request, callbackFunction, timeout=defaultTimeout){
    isRequestSent = false;
    ctr = 0;
    var response = NO_ACTIVE_REPLICAS;
    while(!isRequestSent && ctr++<20){
        if(bestAR == null){
            break;
        }
        try {
            const resp = await fetch(`${bestAR}?name=${appName}&qval=${request}`, {method:"GET", signal: AbortSignal.timeout(5000)});
            var respText = await resp.text();
            respJson = JSON.parse(respText.substring(11));
            isRequestSent = true;
            response = successRequestMsg(respJson["QID"]);
            callbackFunction();
        }
        catch(error){
            console.error("The following error occurred: ");
            console.error(error);
            console.log("Attempting request with the next best replica available");
            bestAR = removeARAndChooseNextBestReplica(request, callbackFunction);
        };
    }
    return response;
}
async function initClient(currAppName="HttpActiveReplicaTestApp0") {
    console.log(currAppName);
    appName = currAppName;
    periodicallyCheckActiveARs();
    if (intervalId === null) {
        console.log("Library initialized. Starting periodic updates...");
        intervalId = setInterval(periodicallyCheckActiveARs, CHECK_ACTIVE_ARs_DURATION);
    }
    return "Client has been initiated successfully";
}
async function closeClient(){
    clearInterval(intervalId);
    intervalId = null;
    return "Client's operations closed successfully";
}