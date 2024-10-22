//import {ReconfigAsyncClient} from "../dist/reconfig-async-client.js";

const inputField = document.getElementById('requestInput');
const initClientButton = document.getElementById('initClientButton');
const sendRequestButton = document.getElementById('sendRequestButton');
const closeClientButton = document.getElementById('closeClientButton');
//console.log(ReconfigAsyncClient)
function initClientButtonClick(){
    const resp = initClient();
    initClientButton.disabled = true;
    if(inputField.value.trim() !== ""){
        sendRequestButton.disabled = false;
    }
    closeClientButton.disabled = false;
    resp.then((r) => addLog(r));
}

function sendRequestButtonClick(){
   const resp = sendRequest(inputField.value,function callbackFunc() {
    alert("Request has been successful");
   });
   resp.then((r) => {
    console.log("r = ", r);
    addLog(r);
   });
}

function closeClientButtonClick(){
    const resp = closeClient();
    initClientButton.disabled = false;
    sendRequestButton.disabled = true;
    closeClientButton.disabled = true;
    resp.then((r) => addLog(r));
}

inputField.addEventListener('input', function() {
    if (inputField.value.trim() !== "" && initClientButton.disabled) {
        sendRequestButton.disabled = false;
    } else {
        sendRequestButton.disabled = true;
    }
});

function addLog(log){
    const responseContainer = document.getElementById('responseContainer');
    responseContainer.innerHTML += `>>> <pre style="white-space: normal;">${new Date(Date.now()).toISOString()} ${log}</pre>`
}
