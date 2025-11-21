// src/index.js

class InetSocketAddress {
    constructor(address, port) {
        this.port = port;
        this.address = address;
        this.url = `http:\/\/${this.address}:${this.port}/`;
      }
}

var ownIpAddr;
var ownIpDetails;
var latenciesMap = new Map();
var isInitialized = new Map();

const CHECK_ACTIVE_ARs_DURATION = 600*1000;
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
function haversineDistance(latOwn, longOwn, latAR, longAR) {
  const R = 6371; // Radius of the Earth in kilometers
  const lat1 = latOwn * (Math.PI / 180);
  const lon1 = longOwn * (Math.PI / 180);
  const lat2 = latAR * (Math.PI / 180);
  const lon2 = longAR * (Math.PI / 180);

  const dLat = lat2 - lat1;
  const dLon = lon2 - lon1;

  const a = Math.sin(dLat / 2) ** 2 +
            Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c; // Distance in kilometers
}

async function measureGeoDistance(arIpAddr){
    const arIpDetails = await fetch(`https://ipapi.co/${arIpAddr}/json/`).then((resp) => resp.json());
    const latOwn = parseFloat(ownIpDetails["latitude"]);
    const longOwn = parseFloat(ownIpDetails["longitude"]);
    const latAR = parseFloat(arIpDetails["latitude"]);
    const longAR = parseFloat(arIpDetails["longitude"]);
    return haversineDistance(latOwn, longOwn, latAR, longAR);
}

function getARWithLeastLatency(latenciesMap){
    return [...latenciesMap.entries()].reduce((a, e) => e[1] < a[1] ? e : a)[0];
}
async function periodicallyCheckActiveARs(serviceName) {
    var rcs = latenciesMap.get(serviceName).get("reconfigurators");
    console.log(rcs)
    var rc = rcs[Math.floor(Math.random()*rcs.length)];
    console.log("rc = ", rc);
    try{
        await fetch(`${rc.url}?type=REQ_ACTIVES&name=${serviceName}`, {method: "GET"})
        .then((resp) => resp.json())
        .then((resp) => {
            latenciesMap.get(serviceName).set("activeReplicaURLs" , resp["ACTIVE_REPLICAS"]);
        });
        console.log(latenciesMap.get(serviceName).get("activeReplicaURLs"));
        latenciesMap.get(serviceName).set("latencies", new Map());
        for(let i=0;i<latenciesMap.get(serviceName).get("activeReplicaURLs").length;i++){
            let comps = latenciesMap.get(serviceName).get("activeReplicaURLs")[i].split(":");
            const offsetURL =  `http:\/\/${comps[0].substring(1)}:${parseInt(comps[1]) + 300}`;

            if(latenciesMap.get(serviceName).get("isLocal") == true){
                let latency = await measureLatency(offsetURL);
                latenciesMap.get(serviceName).get("latencies").set(offsetURL, latency);
            }
            else {
                let geoDistance = await measureGeoDistance(comps[0].substring(1));
                latenciesMap.get(serviceName).get("latencies").set(offsetURL, geoDistance);
            }
        }
        console.log("Current latencies :: ", latenciesMap);
        latenciesMap.get(serviceName).set("bestAR", getARWithLeastLatency(latenciesMap.get(serviceName).get("latencies")));
        console.log("Best latency is for AR :: ", latenciesMap.get(serviceName).get("bestAR"));
    }
    catch (error){
        console.error("The following error occurred : "+error);
        console.log("Trying after 30 seconds.")
    }

}
function removeARAndChooseNextBestReplica(serviceName){
    latenciesMap.get(serviceName).get("latencies").delete(latenciesMap.get(serviceName).get("bestAR"));
    return latenciesMap.get(serviceName).get("latencies").size > 0 ? getARWithLeastLatency(latenciesMap.get(serviceName).get("latencies")) : null;
}


async function sendRequest(serviceName, endpoint, requestSpecs, callbackFunction, onErrorFunction, rcUrls, timeoutDuration=defaultTimeout){
    if(!ownIpAddr){
        ownIpAddr = await fetch("https://api.ipify.org/?format=string").then((resp) => resp.text());
        ownIpDetails = await fetch(`https://ipapi.co/${ownIpAddr}/json/`).then((resp) => resp.json());
        console.log("own ip address = ", ownIpAddr);
    }
    if(!isInitialized.has(serviceName)){
        const service = {
            "name": serviceName,
            "rcUrls": !rcUrls ? ["127.0.0.1:3300", "127.0.0.1:3301", "127.0.0.1:3302"] : rcUrls,
            "isLocal": rcUrls && rcUrls.length > 0 
                        ? (rcUrls[0].includes("127.0.0.1") ? true : false) 
                        : true
        };
        await initService(service);
        isInitialized.set(serviceName, true);
    }
    var isRequestSent = false;
    var ctr = 0;
    var response = NO_ACTIVE_REPLICAS;
    while(!isRequestSent && ctr++<20){
        if(!latenciesMap.get(serviceName).has("bestAR") || latenciesMap.get(serviceName).get("bestAR") == null){
            onErrorFunction(response);
            break;
        }
        try {
            let bestAR = latenciesMap.get(serviceName).get("bestAR");
            //const resp = await fetch(`${bestAR}?name=${serviceName}&qval=${request}`, {method:"GET", signal: AbortSignal.timeout(timeoutDuration)});
            const resp = await fetch(`${bestAR}${endpoint}`, {
                ...requestSpecs, 
                headers: {
                    ...requestSpecs?.headers, 
                    "XDN": serviceName
                },
                signal: AbortSignal.timeout(timeoutDuration)
            });
            if (await resp.status != 200){
                throw new Error(`Active Server ${bestAR} has crashed or not available for service ${serviceName}`);
            }
            response = await resp.json();
            isRequestSent = true;
            callbackFunction(response);
        }
        catch(error){
            console.error("The following error occurred: ");
            console.error(error);
            console.log("Attempting request with the next best replica available");
            latenciesMap.get(serviceName).set("bestAR", removeARAndChooseNextBestReplica(serviceName));
            if (latenciesMap.get(serviceName).get("bestAR") == null){
                console.error("All replicas are inactive, please try after some time.");
            }
        };
    }
    return response;
}

async function initService(service){
        let serviceName = service.name;
        let rcUrls = service.rcUrls;
        let isLocal = service.isLocal;

        
        latenciesMap.set(serviceName, new Map());

        latenciesMap.get(serviceName).set("reconfigurators", rcUrls.map((x) => new InetSocketAddress(x.split(":")[0], x.split(":")[1])));
        latenciesMap.get(serviceName).set("isLocal",isLocal);
        await periodicallyCheckActiveARs(serviceName);
        if (!latenciesMap.get(serviceName).has("intervalId")) {
            console.log("Service initialized. Starting periodic updates...");
            latenciesMap.get(serviceName).set("intervalId", setInterval(function() { periodicallyCheckActiveARs(serviceName); }, CHECK_ACTIVE_ARs_DURATION));
        }
        console.log(latenciesMap);
}
async function closeClient(){
    latenciesMap.keys().forEach((serviceName) =>{
        clearInterval(latenciesMap.get(serviceName).get("intervalId"));
        latenciesMap.get(serviceName).delete("intervalId");
    });
    return "Client's operations closed successfully";
}

module.exports = sendRequest;