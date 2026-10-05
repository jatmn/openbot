// Optional channel-coordination-v1 contract. Existing channels-v1 commands keep their meaning.
import { fields, identifier, list, nullable, type OptionalRouteCodec, oneOf, string } from "./admin-wire";
import { CHANNEL_ROUTES, channelResponse } from "./channels-v1";

export const CHANNEL_COORDINATION_CAPABILITY = "channel-coordination-v1";
export const CHANNEL_COORDINATION_ROUTE = "/v1/channel-coordination/command";
export const CHANNEL_COORDINATION_CODECS: ReadonlyMap<string, OptionalRouteCodec> = new Map([
  [
    CHANNEL_COORDINATION_ROUTE,
    {
      request: fields({
        type: oneOf("coordinate"),
        channelId: identifier,
        operationId: identifier,
        audience: oneOf("lead", "all"),
        text: string(100000),
        replyToMessageId: nullable(identifier),
        attachmentDraftIds: list(identifier, 64),
      }),
      response: (status, value) => channelResponse(CHANNEL_ROUTES.command, status, value),
    },
  ],
]);
