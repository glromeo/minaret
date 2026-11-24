/**
 * The logging surface every class in this package takes, mirroring the short-name aliases
 * Runtime/Utils/LoggerExtensions.cs puts on ILogger — trace/debug/info/warn/error, same five names
 * in the same order of severity.
 *
 * A plain object of five functions rather than an interface with an implementation to construct:
 * there is no DI container here to resolve an ILogger<T> from, so a caller either passes something
 * shaped like this (a pino instance, a console wrapper, its own sink) or takes the default.
 *
 * Call sites use template literals rather than C#'s `{Placeholder}` message templates. Structured
 * templates exist so a provider can index on the fields; nothing downstream of console does, and a
 * caller that wants structure passes its own logger, which is free to interpret the message however
 * it likes.
 */

/** Everything at info and above; debug/trace dropped, matching a default host's filter. */
export const consoleLogger = {
    trace() {},
    debug() {},
    info: (message, ...args) => console.info(message, ...args),
    warn: (message, ...args) => console.warn(message, ...args),
    error: (message, ...args) => console.error(message, ...args)
};

/** For tests and for an embedding that routes diagnostics somewhere of its own. */
export const silentLogger = {
    trace() {},
    debug() {},
    info() {},
    warn() {},
    error() {}
};
