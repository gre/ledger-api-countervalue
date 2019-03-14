// @flow
/* eslint-disable no-console */

import type { PriceUpdate } from "./types";
import querystring from "querystring";

const now = () => new Date().toISOString();

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
    now() +
      " " +
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
    now() +
      " " +
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
  console.log(`${now()} ${request.method} ${request.url}`);
};

export const logEndpointError = (request: *, error: *) => {
  console.log(
    `${now()} ${request.method} ${request.url} =>`,
    conciseHttpError(error),
    error && error.stack
  );
};

export const log = (l: string) => {
  console.log(`${now()} ${l}`);
};

export const logError = (l: string, e: *) => {
  console.error(`${now()} ${l}`, e && e.stack);
};

export const pullLiveRatesDebugMessage = (msgs: PriceUpdate[]) =>
  console.log(
    now() +
      " " +
      msgs.map(msg => `${msg.pairExchangeId} ${msg.price}`).join("\n")
  );

export const pullLiveRatesError = (err: *) =>
  console.error(now() + " pullLiveRatesError", err);

export const pullLiveRatesEnd = () => console.warn(now() + " pullLiveRatesEnd");

export const failRefreshingData = (err: *, id: string) =>
  console.error(`${now()} FAIL REFRESH ${id}: ${conciseHttpError(err)}`);

export const failSyncStats = (err: *) =>
  console.error(`${now()} FAIL sync stats: ${conciseHttpError(err)}`);
