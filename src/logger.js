// @flow
/* eslint-disable no-console */

import type { CoinAPI_TickerMessage } from "./types";

const conciseHttpError = error => {
  if (
    error &&
    error.response &&
    error.response.data &&
    typeof error.response.data.error === "string"
  )
    return error.response.data.error;
  return String((error && error.message) || error);
};

export const logAPI = ({ url, duration, status }: *) => {
  console.log(
    "API call: HTTP " + status + " (" + duration.toFixed(0) + "ms) <= " + url
  );
};

export const logAPIError = ({ url, duration, error }: *) => {
  console.log(
    "API call: ERROR (" +
      duration.toFixed(0) +
      "ms) <= " +
      url +
      ": " +
      conciseHttpError(error)
  );
};

export const logEndpointCall = (request: *) => {
  console.log(`${request.method} ${request.url}`);
};

export const logEndpointError = (request: *, error: *) => {
  console.log(
    `${request.method} ${request.url} =>`,
    conciseHttpError(error),
    error && error.stack
  );
};

export const pullLiveRatesDebugMessage = (msg: CoinAPI_TickerMessage) =>
  console.log(`${msg.taker_side} ${msg.symbol_id} ${msg.price}`, msg);

export const pullLiveRatesError = (err: *) => console.error(err);

export const failRefreshingData = (err: *, id: string) =>
  console.error(`FAIL REFRESH ${id}: ${conciseHttpError(err)}`);
