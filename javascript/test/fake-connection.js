import { Emitter } from "../src/utils/emitter.js";
import { ConnectionState } from "../src/messaging/connection.js";
import { Destination, header } from "../src/messaging/message.js";
import { newGuid } from "../src/messaging/utils/guid.js";
import { serializeMessage, tryParseHeader } from "../src/messaging/utils/wire.js";
import { Verbs } from "../src/messaging/verbs.js";

/**
 * A hand-rolled Connection standing in for the Broker, following the repo's fakes-over-mocks
 * convention and runtime's FakeConnection in particular.
 *
 * Like that one it **auto-acks the register handshake**, since Router.connectTo does not return
 * until the ack lands — and, crucially, it acks from a *later turn* rather than inline on the send.
 * A real Broker's ack arrives on the receive path after the send completed; acking re-entrantly
 * would let the registration settle before `#register` had returned from its own await, hiding the
 * ack-racing-registration ordering Router has to get right.
 *
 * Frames are raised by serializing and re-parsing them, so a test exercises the real codec rather
 * than a shape hand-built to match what the code expects.
 */
export class FakeConnection {

    state = ConnectionState.Closed;

    onFrame = new Emitter();

    onDisconnected = new Emitter();

    /** Every (header, body) handed to send, in order. */
    sent = [];

    disposed = false;

    suppressRegistrationAck = false;

    ackClockOffsetMs = 0;

    /** Verb -> (requestBody, requestHeader) => ({ type, body }) | null, for canned control replies. */
    autoAck = new Map();

    async connect() {
        this.state = ConnectionState.Open;
    }

    async send(head, body) {
        this.sent.push({ header: head, body });

        if (head.type === Verbs.ClientRegister) {
            if (!this.suppressRegistrationAck) this.#later(() => this.ackRegistration(JSON.parse(body)));
            return;
        }

        const canned = this.autoAck.get(head.type);
        if (canned === undefined) return;

        const reply = canned(JSON.parse(body), head);
        if (reply === null) return;

        this.#later(async () => {
            for (const frame of Array.isArray(reply) ? reply : [reply])
                await this.raise(
                    header(frame.type, Destination.fromId(head.from), { from: null, replyTo: frame.replyTo ?? head.replyTo }),
                    frame.body
                );
        });
    }

    dispose() {
        this.disposed = true;
        this.state = ConnectionState.Closed;
    }

    /**
     * The Broker's enriched ClientInfo — the round trip Router.connect is actually awaiting. Mints a
     * RouteId when none was proposed, standing in for the Broker's IdentityRegistry.
     */
    ackRegistration(clientInfo) {
        const routeId = clientInfo.routeId ?? newGuid();
        return this.raise(
            header(Verbs.ClientRegistered, Destination.fromId(routeId), { from: null }),
            { ...clientInfo, routeId, clockOffsetMs: this.ackClockOffsetMs }
        );
    }

    async raise(head, body) {
        const { frame, error } = tryParseHeader(await serializeMessage(head, body));
        if (error !== null) throw new Error(error);

        this.onFrame.emit(frame);
    }

    #later(action) {
        setTimeout(() => void action(), 0);
    }

}

/** Waits for the frame queue to drain — one turn past the last raise the test made. */
export function settle() {
    return new Promise(resolve => setTimeout(resolve, 5));
}
