const {ipcRenderer} = require('electron');

function download() {
    const plz = document.getElementById("plz").value;
    console.log("PLZ: " + plz)
    if (!plz) {return false;}
    ipcRenderer.send('download', plz);
}

function stopandsave() {
    ipcRenderer.send('save');
}

ipcRenderer.on('message', ((event, message) => {
    document.getElementById("text").innerText = message;
}))
