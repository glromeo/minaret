import type { Logger } from "./logger.js";

/** Removes the handler that returned it — the disposer form of `-=`. */
export type Unsubscribe = () => void;

export declare class Emitter<TArgs extends unknown[] = unknown[]> {
    constructor(log?: Logger, name?: string);

    readonly count: number;

    add(handler: (...args: TArgs) => void): Unsubscribe;

    remove(handler: (...args: TArgs) => void): void;

    emit(...args: TArgs): void;
}
