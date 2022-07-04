define(function () {
    let iconManager = {};

    iconManager.connected = () =>
        chrome.browserAction.setIcon({path: "assets/img/icon/icon_48.png"});

    iconManager.disconnected = () =>
        chrome.browserAction.setIcon({path: "assets/img/icon/icon_BW_48.png"});

    return iconManager;
});
