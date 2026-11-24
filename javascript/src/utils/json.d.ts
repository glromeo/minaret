export declare function stringify(value: unknown): string;

/** The parsed body, or undefined when the text isn't JSON — never a throw on a receive path. */
export declare function parse<T = unknown>(text: string): T | undefined;
