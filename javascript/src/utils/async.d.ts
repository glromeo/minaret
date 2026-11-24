export interface Deferred<T> {
    readonly promise: Promise<T>;
    resolve(value: T): void;
    reject(reason?: unknown): void;
}

export declare function createDeferred<T>(signal?: AbortSignal): Deferred<T>;

export declare function delay(ms: number, signal?: AbortSignal): Promise<void>;
