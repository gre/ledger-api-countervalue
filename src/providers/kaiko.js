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

function symbolToExtendedPairExchange(symbol: string): string {
  const tmpSymbolExchange = symbol.toUpperCase().split("/");
  if (
    tmpSymbolExchange.length !== 3 ||
    tmpSymbolExchange[2].split("-").length !== 2
  ) {
    throw new Error(
      `Expecting "symbolExchange" of format : <exchange>/spot/<from>-<to>`
    );
  }
  const [exchange, from, to] = [
    tmpSymbolExchange[0],
    ...tmpSymbolExchange[2].split("-")
  ];
  return {
    exchange,
    from,
    to
  };
}

function pairExchangeIdToSymbol(pairExchangeId: string): string {
  const { from, to, exchange } = pairExchangeFromId(pairExchangeId);
  return `${exchange}/spot/${from}-${to}`.toLowerCase();
}

const KAIKO_KEY = process.env.KAIKO_KEY;
const KAIKO_KEY_WSS = process.env.KAIKO_KEY_WSS;
const KAIKO_REGION = process.env.KAIKO_REGION || "eu";
const KAIKO_API_VERSION = process.env.KAIKO_API_VERSION || "v1";
const KAIKO_WSS_VERSION = "v2"; // Available only for v2
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
  const periodSeconds = granularityMs[granularity] / 1000;

  const symbolExchange = pairExchangeIdToSymbol(id);
  let points: Kaiko_Timeseries[] = (await get(
    `/${KAIKO_API_VERSION}/data/trades.${KAIKO_API_VERSION}/exchanges/${symbolExchange}/aggregations/ohlcv/recent`,
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
        points[points.length - 1].timestamp - periodSeconds * limit
      ).toISOString();
      result = (await get(
        `/${KAIKO_API_VERSION}/data/trades.${KAIKO_API_VERSION}/exchanges/${symbolExchange}/aggregations/ohlcv`,
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
  const { exchange, from, to } = symbolToExtendedPairExchange(symbolExchange);
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
  const symbolExchange = pairExchangeIdToSymbol(id);
  let points: Kaiko_Timeseries[] = (await get(
    `/${KAIKO_API_VERSION}/data/trades.${KAIKO_API_VERSION}/exchanges/${symbolExchange}/aggregations/vwap`,
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
  const { exchange, from, to } = symbolToExtendedPairExchange(symbolExchange);
  return extendedInfos
    ? { exchange, code: [from, to].join("-"), prices }
    : prices;
};

const fetchExchanges = async () => {
  const list: Kaiko_Exchange[] = (await get(`/${KAIKO_API_VERSION}/exchanges`, {
    useRefDataEndpoint: true
  })).data;
  const exchanges = list.map(e => ({
    id: e.code.toUpperCase(),
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
      supportTicker(pairExchange.from) &&
      supportTicker(pairExchange.to)
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
    log("Kaiko WebSocket: create (total=" + websocketTotal + ")");
    let done = false;
    if (websocketTotal >= MAX_WEBSOCKET) {
      log("Kaiko WebSocket: too many WebSocket opened. this should not happen");
      process.exit(1);
    }
    websocketTotal++;
    const wssUrl = `wss://${KAIKO_REGION}.market-ws.kaiko.io/${KAIKO_WSS_VERSION}/rpc`;
    const ws = new WebSocket(wssUrl, ["api_key", KAIKO_KEY_WSS]);
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
        command: "subscribe",
        args: {
          subscriptions: {
            pattern: "*:spot:*", // <exchange>:<instrument_class>:<instrument>
            topic: "trades",
            data_version: "latest"
          }
        }
      };
      ws.send(JSON.stringify(message));
    });
    ws.on("message", data => {
      if (done) return;
      const r = JSON.parse(data);
      if (r && typeof r === "object") {
        if (r.event === "error") {
          o.error(r.payload);
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
                price: r.payload.data[0].price
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
  });

const provider: Provider = {
  init,
  fetchHistoSeries,
  fetchExchanges,
  fetchAvailablePairExchanges,
  subscribePriceUpdate
};

export default provider;
