define(function () {
    let configFactory = {};
    configFactory.getConfigForHosts = function (hosts) {
        hosts = hosts
            .map(atob)
            .map((x) => "HTTPS " + x + ":" + "443")
            .join("; ");

        let config = {
            mode: "pac_script",
            pacScript: {
                data: "function FindProxyForURL(url, host) {\n" +
                "if (host === 'localhost') {" +
                "return 'SYSTEM;';" +
                "}" +
//                "return 'HTTPS cribporksake.info:443';\n" +
                "return '" + hosts + "';\n" +
                "}",
                mandatory: true
            }
        };
		//console.log(config)
        return {value: config};

    };

    configFactory.system = () => ({value: {mode: "system"}});

    configFactory.direct = () => ({value: {mode: "direct"}});

    return configFactory;
});
