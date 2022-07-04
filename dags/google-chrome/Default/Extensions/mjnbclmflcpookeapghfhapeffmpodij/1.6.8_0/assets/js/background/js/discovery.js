define(["jquery", "proxy-control", "proxy-config-factory"], function ($, ProxyController, proxyConfigFactory) {
    "use strict";
    let discovery;

    discovery = {};

    discovery.getHostJSON = function (callback) {
        $.ajax(chrome.extension.getURL("assets/json/d.json"))
            .onSuccess(function (d) {
                d = typeof (d) === "string" ? $.parseJSON(d) : d;
                callback(d);
            });

    };

    discovery.getHosts = function (count, callback) {
        discovery.getHostJSON(function (hosts) {
            let result = [];
            let seen = {};
            for (let i = 0; i < count; i++) {
                let idx = Math.floor(Math.random() * 100000) % hosts.length;
                if (seen[idx] === true) {
                    continue
                }
                seen[idx] = true;
                result.push(hosts[idx])
            }
            callback(result);
        });
    };


    discovery.getProxyController = function (callback) {
        discovery.getHosts(10, function (servers) {
            //console.log("Hosts fetched successfully");
            let rule;
            rule = new ProxyController();
            rule.config = proxyConfigFactory.getConfigForHosts(servers);
            callback(rule);
        });
    };
    return discovery;
});

