define(function () {
    "use strict";
    let ProxyController, noProxy;
    ProxyController = (function () {});
    ProxyController.prototype.config = undefined;

    ProxyController.prototype.enable = function (callback) {
        //console.log("Proxy:", this.config);
        chrome.proxy.settings.set(this.config, function () {
            if (typeof(callback) === "function") {
                callback(true);
            }
        });
    };
    ProxyController.disable = function (callback) {
        chrome.proxy.settings.clear({}, function () {
            if (typeof(callback) === "function") {
                callback(true);
            }
        });
    };

    ProxyController.prototype.disable = ProxyController.disable;

    noProxy = new ProxyController();
    noProxy.enable = ProxyController.disable;

    ProxyController.noProxy = noProxy;

    return ProxyController
});
