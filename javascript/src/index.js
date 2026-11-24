/**
 * The package's public surface. There is no counterpart in runtime/ — Java has packages and a jar
 * boundary where this has one entry point — so the layout below is the one place this client answers
 * a question runtime never has to.
 *
 * Everything is also reachable at its own path (`@messagebus/client/messaging/router`,
 * `@messagebus/client/discovery`, …), which is what keeps a file here a file there.
 */

export { ChannelBase, ChannelClient, ChannelProvider, MessageBus } from "./messaging/channels.js";
export { CloseCodes, ConnectionState } from "./messaging/connection.js";
export { EventNames, EventOrigin, replayOccurrences } from "./messaging/events.js";
export { Destination, header } from "./messaging/message.js";
export { newClientInfo, Router } from "./messaging/router.js";
export { decodeGuid, encodeGuid, newGuid } from "./messaging/utils/guid.js";
export { BodyEncoding, COMPRESSION_THRESHOLD_BYTES, TAG } from "./messaging/utils/wire.js";
export { Verbs } from "./messaging/verbs.js";
export { WebSocketConnection } from "./messaging/websocket-connection.js";
export { DEFAULT_REALM, resolveRealm } from "./discovery/realm.js";
export { consoleLogger, silentLogger } from "./utils/logger.js";
