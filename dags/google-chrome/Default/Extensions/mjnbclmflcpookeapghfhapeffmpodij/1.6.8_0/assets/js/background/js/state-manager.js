var intervalID=!1,iconState=0;
function setIcon(b){"connecting"===b&&!1===intervalID?(iconState=1,chrome.browserAction.setIcon({path:"assets/img/icon/signal/0.png"}),intervalID=setInterval(function(){0==iconState?(chrome.browserAction.setIcon({path:"assets/img/icon/signal/0.png"}),iconState=1):1==iconState?(chrome.browserAction.setIcon({path:"assets/img/icon/signal/1.png"}),iconState=2):2==iconState?(chrome.browserAction.setIcon({path:"assets/img/icon/signal/2.png"}),iconState=3):3==iconState&&(chrome.browserAction.setIcon({path:"assets/img/icon/signal/3.png"}),iconState=0)},400)):intervalID&&(clearInterval(intervalID),intervalID=!1);"connected"===b&&chrome.browserAction.setIcon({path:"assets/img/icon/icon_48.png"});"ready"===b&&chrome.browserAction.setIcon({path:"assets/img/icon/icon_BW_48.png"});"noConnection"===b&&chrome.browserAction.setIcon({path:"assets/img/icon/icon_error_48.png"})}function e(b){"enable"!=b&&!0!==b||chrome.storage.local.set({enabled:!0});"disable"!=b&&!1!==b||chrome.storage.local.set({enabled:!1})}

define(["icon-manager"],
    function (iconManager) {

        "use strict";
        let stateManager, updateStatus;
        stateManager = {};
        stateManager.set = function (state) {
            state = localStorage["enabled"] === "true" || state === "conflict" ? state : "disconnect";
            //console.log("Setting state:", state);

            switch (state) {
                case "success":
                    setSuccess(state);
                    break;
                case "disconnect":
                    setDisconnect(state);
                    break;
                default:
                    //console.warn("state-manager.set() State not recognized", state);
            }
        };

        updateStatus = (s) => localStorage["state"] = s;

        let setSuccess = function (event) {
            updateStatus(event);
			setIcon("connecting")
			verify("init")
            //iconManager.connected();
            //iconManager.connected();
        };
        let setDisconnect = function (event) {
            updateStatus(event);
            iconManager.disconnected();
        };

        return stateManager;
    });
