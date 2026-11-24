/**
 * A multicast callback list, mirroring runtime's Emitter.
 *
 * Keeping it a small object rather than extending EventTarget preserves the two properties its
 * users rely on — handlers run in registration order, and synchronously — which an EventTarget's
 * dispatch does not promise in a form worth depending on.
 *
 * One handler throwing must not cost the others their turn (a topic handler and an ack correlator
 * are routinely on the same list), so a throw is reported to the logger and the walk continues.
 */
export class Emitter {

    #handlers = [];

    constructor(log, name) {
        this.log = log;
        this.name = name;
    }

    get count() {
        return this.#handlers.length;
    }

    add(handler) {
        this.#handlers.push(handler);
        return () => this.remove(handler);
    }

    remove(handler) {
        const index = this.#handlers.indexOf(handler);
        if (index >= 0) this.#handlers.splice(index, 1);
    }

    /**
     * Iterates a snapshot, so a handler that unsubscribes itself (or subscribes another) mid-emit
     * doesn't shift the list out from under the walk — which the request/ack helpers do on every
     * single round trip, since each one removes its correlator from inside the emit that resolved
     * it.
     */
    emit(...args) {
        for (const handler of [...this.#handlers]) {
            try {
                handler(...args);
            } catch (error) {
                this.log?.error(`${this.name} handler threw`, error);
            }
        }
    }

}
