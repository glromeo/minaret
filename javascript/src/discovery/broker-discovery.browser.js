/**
 * The browser half of discovery, selected by the `browser` export condition — see the Node half in
 * broker-discovery.js for what this replaces and why the two exist.
 *
 * There is nothing to discover here. The Broker serves the page, so the socket is same-origin by
 * construction; in development a dev server proxies /msgbus through to it, which is why the scheme
 * is derived from location.protocol (the dev server is http, the Broker https) rather than being
 * fixed to wss. That one derivation is what makes the same URL correct in both modes with no
 * build-time switch.
 */

export function pipeName() {
    throw new Error("Named-pipe discovery is not available in a browser.");
}

export function pipePath() {
    throw new Error("Named-pipe discovery is not available in a browser.");
}

export function discover() {
    return Promise.reject(new Error("Named-pipe discovery is not available in a browser; the Broker is same-origin."));
}

/**
 * The realm argument is accepted and ignored, deliberately: a page is served by exactly one Broker,
 * so its realm is already decided by whoever served it, and rejecting the argument would make the
 * two halves of this module differ in signature for no gain.
 */
export function resolveBrokerUrl() {
    const scheme = location.protocol === "https:" ? "wss:" : "ws:";

    return Promise.resolve(`${scheme}//${location.host}/msgbus`);
}
