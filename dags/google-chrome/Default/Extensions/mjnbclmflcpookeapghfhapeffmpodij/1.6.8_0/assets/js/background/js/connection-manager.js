define(["proxy-control", "discovery", "state-manager"],
    function (ProxyController, discovery, stateManager) {
        "use strict";

        let connect, disconnect;

        connect = function (callback) {
            localStorage["enabled"] = true;
            ga('send', 'event', 'connection-manager', 'connect', {
			  hitCallback: createFunctionWithTimeout(function() { 
                discovery.getProxyController(function (controller) {
                    controller.enable(function () {
                        stateManager.set("success");
                        if (callback !== undefined) {
                            callback(true);
                        }
                    });
                });
    		  })
			});
        };

        disconnect = function (callback) {
            localStorage["enabled"] = false;
            ProxyController.disable(function () {
                stateManager.set("disconnect");
                if (callback !== undefined) {
                    callback(true);
                }
            	ga('send', 'event', 'connection-manager', 'disconnect');
            });
        };

        return {
            connect: connect,
            disconnect: disconnect
        };
    });
