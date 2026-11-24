/// <reference types="vite/client" />

/**
 * `idl.grammar` is loaded as text (`?raw`) and parsed at startup by `src/idl/grammar.ts` — Vite's
 * own types cover `?raw` for known extensions, not for one it has never seen.
 */
declare module "*.grammar?raw" {
    const content: string;
    export default content;
}
