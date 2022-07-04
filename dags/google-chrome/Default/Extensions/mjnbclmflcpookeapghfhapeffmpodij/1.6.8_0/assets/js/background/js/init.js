define(["state-manager", "connection-manager"], function (stateManager, connectionManager) {
    "use strict";
    let init;


    init = function () {
        let value = parseInt(localStorage['runCount']);
        let runCount;

        runCount = (value && value > 0 ? value : 0) + 1;
        localStorage['runCount'] = runCount;

        value = parseInt(localStorage['enableCount']);
        localStorage['enableCount'] = (value && value > 0 ? value : 0);

        let enabled = localStorage["enabled"] !== "false";
        localStorage["enabled"] = enabled;

        if (enabled) {
            ga('send', 'event', 'init-connection', 'enabled');
            connectionManager.connect();
        } else {
            ga('send', 'event', 'init-connection', 'disabled');
            connectionManager.disconnect();
        }
        ga('send', 'event', 'initialization', 'version: ' + chrome.runtime.getManifest().version);

        chrome.proxy.settings.get({}, (x) => ga('send', 'event', 'level-of-control', x.levelOfControl));
    };

    return init;
});
