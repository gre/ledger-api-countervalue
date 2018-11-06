// @flow

import type { PairExchange } from "./types";
import {
  listCryptoCurrencies,
  listFiatCurrencies
} from "@ledgerhq/live-common/lib/currencies";
import type { Currency } from "@ledgerhq/live-common/lib/types";
import throttle from "lodash/throttle";

// a promise aware throttle
export const promiseThrottle = <F: Function>(fn: F, ms: number): F => {
  const throttleFn = throttle(async (...args) => {
    try {
      const res = await fn(...args);
      return res;
    } catch (e) {
      throttleFn.cancel();
      throw e;
    }
  }, ms);
  // $FlowFixMe
  return throttleFn;
};

const promisify = <T, A>(inst: *, method: string, ...args: A): Promise<T> =>
  new Promise((res, rej) =>
    inst[method].call(inst, ...args, (err, obj) => {
      if (err) rej(err);
      else res(obj);
    })
  );
export { promisify };

export const recurrentJob = async (fn: Function, ms: number) => {
  await fn();
  setTimeout(() => recurrentJob(fn, ms), ms);
};

const currencies = listCryptoCurrencies();
const fiats = listFiatCurrencies();
export const all = currencies.concat(fiats);
export const currencyTickers = listCryptoCurrencies().map(c => c.ticker);
export const fiatTickers = listFiatCurrencies().map(c => c.ticker);
export const allTickers = currencyTickers.concat(fiatTickers);

export const getFiatOrCurrency = (ticker: string): Currency => {
  const res = all.find(o => ticker === o.ticker);
  if (!res) {
    throw new Error(
      "ticker not found " + ticker + ". should filter with allTickers first"
    );
  }
  return res;
};

export const supportTicker = (ticker: string) => allTickers.includes(ticker);

export const lenseMagnitude = (cur: Currency): number => cur.units[0].magnitude;

export const convertToCentSatRate = (
  from: Currency,
  to: Currency,
  value: number
): number => value * 10 ** (lenseMagnitude(to) - lenseMagnitude(from));

export const pairExchange = (
  exchange: string,
  from: string,
  to: string
): PairExchange => ({ id: `${exchange}_${from}_${to}`, exchange, from, to });

export const pairExchangeFromId = (id: string): PairExchange => {
  const parts = id.split("_");
  return { id, exchange: parts[0], from: parts[1], to: parts[2] };
};

const twoDigits = (n: number) => (n > 9 ? `${n}` : `0${n}`);

export const formatDay = (d: Date) =>
  `${d.getFullYear()}-${twoDigits(d.getMonth() + 1)}-${twoDigits(d.getDate())}`;
