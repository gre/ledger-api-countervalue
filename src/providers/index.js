// @flow

import coinapi from "./coinapi";
import cryptocompare from "./cryptocompare";
import kaiko from "./kaiko"

import type { Provider } from "../types";

export const providers: { [_: string]: Provider } = {
  coinapi,
  cryptocompare,
  kaiko
};

export const getCurrentProvider = (): Provider => {
  const key = process.env.PROVIDER || "coinapi";
  const provider = providers[key];
  if (!provider) {
    throw new Error(`provider '${key}' not found`);
  }
  return provider;
};
