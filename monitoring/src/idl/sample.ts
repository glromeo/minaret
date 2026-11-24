/**
 * What a new API window opens with: a document exercising enough of `idl.grammar` to be worth
 * editing rather than replacing.
 *
 * - `submit` — parameters and a non-null `outbound`, so request/reply and therefore correlated;
 * - `cancel` — parameters and `outbound: null`, so fire-and-forget;
 * - `quote` — `inbound: {}`, since a topic that takes nothing still says so, and a tuple out.
 *
 * `types` shows both kinds of entry: `Order` is a shape, `Side` and `Spread` are aliases — an enum
 * and a tuple. The key of each topic is the identifier generated code names its method after; the
 * `name` inside is what travels on the wire, which is why `quote` and `"OrderQuote"` differ.
 */
export const SAMPLE_IDL = `[
  {
    "name": "Trading.Orders",
    "namespace": "Trading.Orders",
    "types": {
      "Side": "string[Buy|Sell]",
      "Spread": "[number, number]",
      "Order": {
        "symbol": "string",
        "quantity": "number",
        "side": "Side",
        "placedAt": "date"
      },
      "Ack": {
        "$extends": "Order",
        "orderId": "string"
      },
      "Book": {
        "open": "array<Ack>",
        "depth": "object<string, number>"
      }
    },
    "topics": {
      "submit": {
        "name": "Submit",
        "inbound": { "order": "Order" },
        "outbound": "Ack"
      },
      "cancel": {
        "name": "Cancel",
        "inbound": { "orderId": "string", "reason": "string" },
        "outbound": null
      },
      "quote": {
        "name": "OrderQuote",
        "inbound": {},
        "outbound": "Spread"
      }
    }
  }
]
`;
