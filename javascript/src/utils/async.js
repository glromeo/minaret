/**
 * The two async primitives this package would otherwise spell out at every call site. runtime has no
 * counterpart file because CompletableFuture supplies both.
 *
 * AbortSignal is the cancellation carrier throughout, so everything that takes an optional
 * `{ signal }` bounds the wait with it — including the contract that *no* signal means no implicit
 * timeout, which is deliberate:
 * bounding a registration or a request is the caller's choice to make.
 */

/**
 * A promise plus its resolvers, rejected if the signal aborts. The abort listener is removed when it
 * settles, so a long-lived signal (a connection's own, typically) doesn't accumulate one listener
 * per request made under it.
 */
export function createDeferred(signal) {
    let resolve;
    let reject;

    const promise = new Promise((resolveFn, rejectFn) => {
        resolve = resolveFn;
        reject = rejectFn;
    });

    // A rejection handler is attached up front so an abort can never surface as an *unhandled*
    // rejection: every caller creates its deferred before sending the request it correlates, and
    // the send is itself abortable — so an abort landing in between unwinds that caller through the
    // send's own throw, leaving this promise rejected with nobody having reached the await yet. The
    // rejection is still delivered to anyone who does await it; a handler only marks it observed.
    promise.catch(() => {});

    if (signal === undefined || signal === null) return { promise, resolve, reject };

    if (signal.aborted) {
        reject(signal.reason ?? new Error("aborted"));
        return { promise, resolve, reject };
    }

    const onAbort = () => reject(signal.reason ?? new Error("aborted"));
    signal.addEventListener("abort", onAbort, { once: true });

    // Removed however it settles, so a long-lived signal — a connection's own, typically — doesn't
    // accumulate one listener per request made under it.
    const detach = () => signal.removeEventListener("abort", onAbort);
    promise.then(detach, detach);

    return { promise, resolve, reject };
}

export function delay(ms, signal) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(resolve, ms);

        signal?.addEventListener("abort", () => {
            clearTimeout(timer);
            reject(signal.reason ?? new Error("aborted"));
        }, { once: true });
    });
}
