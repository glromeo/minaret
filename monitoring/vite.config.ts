import preact from "@preact/preset-vite";
import { defineConfig } from "vite";

/**
 * The build lands in broker/wwwroot/monitor, which the broker already serves as static content — so
 * a `npm run build` is the whole deployment step and the broker needs no configuration change. The
 * subdirectory keeps the generated output entirely separate from the tracked placeholder page at
 * the wwwroot root, so one directory is authored and the other is generated, with no overlap for
 * .gitignore to straddle. emptyOutDir is explicit because the directory sits outside Vite's root
 * and it refuses to clear such a directory silently.
 *
 * base must match, or the built index.html would request its assets from /assets/... at the server
 * root rather than from /monitor/assets/... where they actually land.
 *
 * In dev the app runs on Vite's own server instead, and /msgbus is proxied to the Broker so the
 * page's WebSocket URL is same-origin in both modes — see resolveBrokerUrl() in the client runtime's
 * discovery/broker-discovery.browser.js.
 * secure:false accepts the local dev certificate (trusting it in the OS store covers the browser,
 * but not the proxy's own Node-side TLS check).
 */
export default defineConfig({
    plugins: [preact()],
    base: "/monitor/",
    build: {
        outDir: "../broker/wwwroot/monitor",
        emptyOutDir: true
    },
    server: {
        proxy: {
            "/msgbus": {
                target: "wss://localhost:5443",
                ws: true,
                secure: false
            },
            // The IDL repository, for the same reason and with the same effect: the API editor
            // fetches a relative /idl in both modes, so nothing in src/ knows which one it is in.
            "/idl": {
                target: "https://localhost:5443",
                secure: false
            }
        }
    }
});
