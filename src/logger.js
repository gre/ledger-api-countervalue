// @flow
/* eslint-disable no-console */

import type { PriceUpdate } from "./types";
import querystring from "querystring";

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

export const logAPI = ({ api, url, duration, opts, status }: *) => {
  const queryString =
    opts && opts.params ? querystring.stringify(opts.params) : "";
  console.log(
    api +
      " call: HTTP " +
      status +
      " (" +
      duration.toFixed(0) +
      "ms) <= " +
      url +
      (queryString ? "?" + queryString : "")
  );
};

export const logAPIError = ({ api, url, duration, opts, error }: *) => {
  const queryString =
    opts && opts.params ? querystring.stringify(opts.params) : "";
  console.log(
    api +
      " call: ERROR (" +
      duration.toFixed(0) +
      "ms) <= " +
      url +
      (queryString ? "?" + queryString : "") +
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

export const pullLiveRatesDebugMessage = (msgs: PriceUpdate[]) =>
  console.log(msgs.map(msg => `${msg.pairExchangeId} ${msg.price}`).join("\n"));

export const pullLiveRatesError = (err: *) =>
  console.error("pullLiveRatesError", err);

export const pullLiveRatesEnd = () => console.warn("pullLiveRatesEnd");

export const failRefreshingData = (err: *, id: string) =>
  console.error(`FAIL REFRESH ${id}: ${conciseHttpError(err)}`);
