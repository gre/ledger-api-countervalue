//@flow

import { Observable } from "rxjs";
import WebSocket from "ws";
import axios from "axios";
import {
  allTickers,
  supportTicker,
  pairExchange,
  pairExchangeFromId,
  granularityMs
} from "../utils";
import { logAPI, logError, logAPIError, log } from "../logger";
import type { PairExchange, Provider, Granularity } from "../types";

type Kaiko_TickerMessage = {|
  timestamp: string,
  uuid: string,
  price: number,
  amount: number,
  taker_side: "SELL" | "BUY",
  symbol_id: string
|};

type Kaiko_Symbol = {|
  code: string,
  exchange_code: string,
  class: string,
  base_asset: string,
  quote_asset: string
|};

type Kaiko_Exchange = {|
  code: string,
  name: string
|};

type Kaiko_Timeseries = {|
  timestamp: string,
  open: string,
  close: string,
  open: number,
  high: number,
  low: number,
  close: number,
  volume: number
|};

type Kaiko_Exchange_Symbol = {|
  exchange: string,
  code: string
|};

function symbolToPairExchange(symbol: Kaiko_Exchange_Symbol): ?PairExchange {
  const [from, to] = symbol.code.split("-");
  return pairExchange(
    symbol.exchange.toUpperCase(),
    from.toUpperCase(),
    to.toUpperCase()
  );
}

function pairExchangeIdToSymbol(pairExchangeId: string): string {
  const { from, to, exchange } = pairExchangeFromId(pairExchangeId);
  return `${exchange}/spot/${from}-${to}`.toLowerCase();
}

const KAIKO_KEY = process.env.KAIKO_KEY;
const KAIKO_KEY_WSS = process.env.KAIKO_KEY_WSS;
const KAIKO_REGION = process.env.KAIKO_REGION || "eu";
const KAIKO_API_VERSION = process.env.KAIKO_API_VERSION || "v1";
const KAIKO_WSS_VERSION = process.env.KAIKO_WSS_VERSION || "v1";
const USE_KAIKO_WSS = process.env.USE_KAIKO_WSS || false;

function init() {
  if (!KAIKO_KEY) throw new Error("KAIKO_KEY env is not defined");
  if (USE_KAIKO_WSS && !KAIKO_KEY_WSS) {
    throw Error("To use Kaiko's WSS you need to set KAIKO_KEY_WSS");
  }
}

const get = async (url: string, opts?: *) => {
  const beforeTime = Date.now();
  try {
    const baseURL = opts.useRefDataEndpoint
      ? "https://reference-data-api.kaiko.io"
      : `https://${KAIKO_REGION}.market-api.kaiko.io`;
    const res = await axios.get(`${baseURL}${url}`, {
      ...opts,
      timeout: 50000,
      headers: {
        "X-Api-Key": KAIKO_KEY
      }
    });
    logAPI({
      api: "Kaiko",
      url,
      opts,
      duration: Date.now() - beforeTime,
      status: res.status
    });
    return res.data;
  } catch (error) {
    logAPIError({
      api: "Kaiko",
      error,
      url,
      opts,
      duration: Date.now() - beforeTime
    });
    throw error;
  }
};

const granMap = {
  daily: "1d",
  hourly: "1h"
};

const fetchHistoSeries = async (
  id: string,
  granularity: Granularity,
  limit: number = 10000,
  extendedInfos: boolean = false
) => {
  const interval = granMap[granularity];
  const periodMs = granularityMs[granularity];

  const symbolEchange = pairExchangeIdToSymbol(id);
  let points: Kaiko_Timeseries[] = (await get(
    `/${KAIKO_API_VERSION}/data/trades.${KAIKO_API_VERSION}/exchanges/${symbolEchange}/aggregations/ohlcv/recent`,
    {
      params: {
        interval,
        limit
      }
    }
  )).data;

  if (points.length > 0) {
    let result: Kaiko_Timeseries[] = points;
    let MAX_ITERATION_FAILSAFE = 100;
    let i = 0;
    do {
      const end_time = new Date(
        points[points.length - 1].timestamp
      ).toISOString();
      const start_time = new Date(
        new Date(end_time) - periodMs * limit
      ).toISOString();
      result = (await get(
        `/${KAIKO_API_VERSION}/data/trades.${KAIKO_API_VERSION}/exchanges/${symbolEchange}/aggregations/ohlcv`,
        {
          params: {
            interval,
            start_time,
            end_time
          }
        }
      )).data;
      points = points.concat(result.reverse());
      if (i++ > MAX_ITERATION_FAILSAFE) {
        logError("fetchHistoSeries max iteration failsafe reached!");
        break;
      }
    } while (result.length > 0);
  }

  const timeSeries = points.map(d => ({
    time: new Date(d.timestamp),
    open: d.open,
    high: d.high,
    low: d.low,
    close: d.close,
    volume: d.volume
  }));
  const [exchange, from, to] = symbolEchange.split("_");
  return extendedInfos
    ? { exchange, code: [from, to].join("-"), timeSeries }
    : timeSeries;
};

const fetchPrices = async (
  id: string,
  granularity: Granularity,
  extendedInfos: boolean = false
) => {
  const interval = granMap[granularity];
  const symbolEchange = pairExchangeIdToSymbol(id);
  let points: Kaiko_Timeseries[] = (await get(
    `/${KAIKO_API_VERSION}/data/trades.${KAIKO_API_VERSION}/exchanges/${symbolEchange}/aggregations/vwap`,
    {
      params: {
        interval
      }
    }
  )).data;

  const prices = points.map(d => ({
    time: new Date(d.timestamp),
    price: d.price
  }));
  const [exchange, from, to] = symbolEchange.split("_");
  return extendedInfos
    ? { exchange, code: [from, to].join("-"), prices }
    : prices;
};

const fetchExchanges = async () => {
  const list: Kaiko_Exchange[] = (await get(`/${KAIKO_API_VERSION}/exchanges`, {
    useRefDataEndpoint: true
  })).data;
  const exchanges = list.map(e => ({
    id: e.code,
    name: e.name,
    website: ""
  }));
  return exchanges;
};

const fetchAvailablePairExchanges = async () => {
  let list: Kaiko_Symbol[] = (await get(`/${KAIKO_API_VERSION}/instruments`, {
    useRefDataEndpoint: true
  })).data;
  if (typeof list === "string") {
    throw new Error(
      `/${KAIKO_API_VERSION}/symbols payload is invalid! Got a string!`
    );
  }

  // No filter possible on SPOTS, so first we filter ...
  list = list.filter(instrument => instrument.class === "spot");

  const pairExchanges = [];
  for (const item of list) {
    const pairExchange = symbolToPairExchange({
      exchange: item.exchange_code,
      code: item.code
    });
    if (
      pairExchange &&
      supportTicker(pairExchange.from.toUpperCase()) &&
      supportTicker(pairExchange.to.toUpperCase())
    ) {
      pairExchanges.push(pairExchange);
    }
  }

  return pairExchanges;
};

let websocketTotal = 0;
let MAX_WEBSOCKET = 2;
let currentExchange = 0,
  currentInstrument = 0;
let exchanges = [],
  instruments = [];
let queryTimeout;
const subscribePriceUpdate = () =>
  Observable.create(o => {
    if (!USE_KAIKO_WSS) {
      // Not available yet, but implemented it
      // If we want to test we should have a handshake
      // with an HTTP call with headers
      // Connection: "Upgrade",
      // Upgrade: "websocket",
      // "Sec-WebSocket-Key": KAIKO_KEY,
      log("Kaiko WebSocket: unavailable ...");

      const queryTimeSeries = async () => {
        // Get available exchanges
        if (exchanges.length === 0) {
          exchanges = await fetchExchanges().catch(err =>
            o.error(`Failed to retrieve exchanges: ${err}`)
          );

          //Get available instrument_class
          (await fetchAvailablePairExchanges().catch(err => {
            o.error(`Failed to retrieve instruments: ${err}`);
          })).map(instrument => {
            if (!instruments[instrument.exchange_code]) {
              instruments[instrument.exchange_code] = [];
            }
            // Check if we are interested into those currencies
            if (
              allTickers.includes(instrument.from) &&
              allTickers.includes(instrument.to)
            ) {
              instruments[instrument.exchange_code].push(instrument.code);
            }
          });
        }

        if (exchanges.length === 0 || instruments.length === 0) {
          o.error("Exchanges or instruments are missing");
          return;
        }
        // Update tickers & exchange
        let exchange = exchanges[currentExchange];
        if (currentInstrument === instruments[exchange].length - 1) {
          currentExchange++;
          currentInstrument = 0;
        } else {
          currentInstrument++;
        }
        currentExchange %= exchanges.length;
        exchange = exchanges[currentExchange];
        const [from, to] = instruments[exchange][currentInstrument].split("-");
        const symbolExchanged = `${exchange.id}_${from}_${to}`;
        const withExtendedInfos = true;
        const fetchedPrices = await fetchPrices(
          symbolExchanged,
          "hourly",
          1000,
          withExtendedInfos
        ).catch(e => {
          clearTimeout(queryTimeout);
          o.error(e);
          return;
        });

        if (fetchedPrices && fetchedPrices.length > 0) {
          // Call next Observable
          const messageToNext = prices => {
            const symbol = {
              exchange: prices.exchange,
              code: prices.code
            };
            const result = [];
            for (const price of prices.prices) {
              const maybePairExchange = symbolToPairExchange(symbol);
              if (maybePairExchange) {
                result.push({
                  pairExchangeId: maybePairExchange.id,
                  price: price.price
                });
              }
            }
            return result;
          };
          o.next(messageToNext(fetchedPrices));
        }
        queryTimeout = setTimeout(queryTimeSeries, 2000);
      };

      queryTimeSeries().catch(err => {
        throw new Error(`Failed to query Time series: ${err}`);
      });
      return;
    } else {
      // This is the implementation of Kaiko's websocket
      // once, the feature ready we can uncomment and use it

      log("Kaiko WebSocket: create (total=" + websocketTotal + ")");
      let done = false;
      if (websocketTotal >= MAX_WEBSOCKET) {
        log("Kaiko WebSocket: too many WebSocket opened. this should not happen");
        process.exit(1);
      }
      websocketTotal++;
      const wssUrl = `wss://${KAIKO_REGION}-beta.market-ws.kaiko.io/${KAIKO_WSS_VERSION}`;
      const ws = new WebSocket(
        wssUrl,
        ["api_key", KAIKO_KEY_WSS]
      );
      ws.on("open", () => {
        if (done) {
          try {
            ws.close();
          } catch (e) {
            logError("failed to close WebSocket", e);
          }
          return;
        }
        log("Kaiko WebSocket: open");

        // No other choice than using wildcards here, otherwise using same connection overwrites subscription
        const message = {
          "command": "subscribe",
          "args": {
            "subscriptions": [
              {
                //"Sec-WebSocket-Key": `${KAIKO_KEY_WSS}`,
                "pattern": "*:spot:*", // <exchange>:<instrument_class>:<instrument>
                "topic": "trades",
                "data_version": `${KAIKO_WSS_VERSION}`
              }
            ]
          }
        }
        ws.send(
          JSON.stringify(message)
        );
      });
      ws.on("message", data => {
        if (done) return;
        const r = JSON.parse(data);
        if (r && typeof r === "object") {
          if (r.type === "error") {
            o.error(r.message);
            ws.close();
          } else {
            if (r.event === "update") {
              const symbol = {
                exchange: r.payload.subscription.exchange,
                code: r.payload.subscription.instrument
              };
              const maybePairExchange = symbolToPairExchange(symbol);
              if (maybePairExchange) {
                o.next({
                  pairExchangeId: maybePairExchange.id,
                  price: r.payload.data.price
                });
              }
            }
          }
        }
      });
      ws.on("close", () => {
        log("Kaiko WebSocket: close");
        done = true;
        o.complete();
        websocketTotal--;
      });

      function unsubscribe() {
        log("Kaiko WebSocket: unsubscribe");
        done = true;
        try {
          ws.close();
        } catch (e) {
          logError("failed to close WebSocket", e);
        }
      }

      return { unsubscribe };
    }
  });

const provider: Provider = {
  init,
  fetchHistoSeries,
  fetchExchanges,
  fetchAvailablePairExchanges,
  subscribePriceUpdate
};

export default provider;
