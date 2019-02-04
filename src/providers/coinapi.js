//@flow

import { Observable } from "rxjs";
import WebSocket from "ws";
import axios from "axios";
import {
  cryptoTickers,
  allTickers,
  supportTicker,
  pairExchange,
  pairExchangeFromId,
  granularityMs
} from "../utils";
import { logAPI, logError, logAPIError, log } from "../logger";
import type { PairExchange, Provider, Granularity } from "../types";

type CoinAPI_TickerMessage = {|
  time_exchange: string,
  time_coinapi: string,
  uuid: string,
  price: number,
  size: number,
  taker_side: "SELL" | "BUY",
  symbol_id: string,
  sequence: number,
  type: string
|};

type CoinAPI_Symbol = {|
  symbol_id: string,
  exchange_id: string,
  symbol_type: string,
  asset_id_base: string,
  asset_id_quote: string
|};

type CoinAPI_Exchange = {|
  exchange_id: string,
  website: string,
  name: string,
  data_start: string,
  data_end: string,
  data_quote_start: string,
  data_quote_end: string,
  data_orderbook_start: string,
  data_orderbook_end: string,
  data_trade_start: string,
  data_trade_end: string,
  data_trade_count: number,
  data_symbols_count: number
|};

type CoinAPI_Timeseries = {|
  time_period_start: string,
  time_period_end: string,
  time_open: string,
  time_close: string,
  price_open: number,
  price_high: number,
  price_low: number,
  price_close: number,
  volume_traded: number,
  trades_count: number
|};

function symbolToPairExchange(symbol: string): ?PairExchange {
  const [exchange, type, from, to] = symbol.split("_");
  if (type !== "SPOT") return;
  return pairExchange(exchange, from, to);
}

function pairExchangeIdToSymbol(pairExchangeId: string): string {
  const { from, to, exchange } = pairExchangeFromId(pairExchangeId);
  return `${exchange}_SPOT_${from}_${to}`;
}

const COINAPI_KEY = process.env.COINAPI_KEY;

function init() {
  if (!COINAPI_KEY) throw new Error("COINAPI_KEY env is not defined");
}

const get = async (url: string, opts?: *) => {
  const beforeTime = Date.now();
  try {
    const res = await axios.get(`https://rest.coinapi.io${url}`, {
      ...opts,
      timeout: 50000,
      headers: {
        "X-CoinAPI-Key": COINAPI_KEY
      }
    });
    logAPI({
      api: "CoinAPI",
      url,
      opts,
      duration: Date.now() - beforeTime,
      status: res.status
    });
    return res.data;
  } catch (error) {
    logAPIError({
      api: "CoinAPI",
      error,
      url,
      opts,
      duration: Date.now() - beforeTime
    });
    throw error;
  }
};

const granMap = {
  daily: "1DAY",
  hourly: "1HRS"
};

const fetchHistoSeries = async (
  id: string,
  granularity: Granularity,
  limit: number = 10000
) => {
  const period_id = granMap[granularity];
  const periodMs = granularityMs[granularity];

  let points: CoinAPI_Timeseries[] = await get(
    `/v1/ohlcv/${pairExchangeIdToSymbol(id)}/latest`,
    {
      params: {
        period_id,
        limit
      }
    }
  );

  if (points.length > 0) {
    let result: CoinAPI_Timeseries[] = points;
    let MAX_ITERATION_FAILSAFE = 100;
    let i = 0;
    do {
      const time_end = points[points.length - 1].time_period_start;
      const time_start = new Date(
        new Date(time_end) - periodMs * limit
      ).toISOString();
      result = await get(`/v1/ohlcv/${pairExchangeIdToSymbol(id)}/history`, {
        params: {
          period_id,
          time_start,
          time_end
        }
      });
      points = points.concat(result.reverse());
      if (i++ > MAX_ITERATION_FAILSAFE) {
        logError("fetchHistoSeries max iteration failsafe reached!");
        break;
      }
    } while (result.length > 0);
  }

  const timeSeries = points.map(d => ({
    time: new Date(d.time_period_start),
    open: d.price_open,
    high: d.price_high,
    low: d.price_low,
    close: d.price_close,
    volume: d.volume_traded
  }));
  return timeSeries;
};

const fetchExchanges = async () => {
  const list: CoinAPI_Exchange[] = await get("/v1/exchanges");
  const exchanges = list.map(e => ({
    id: e.exchange_id,
    name: e.name,
    website: e.website
  }));
  return exchanges;
};

const fetchAvailablePairExchanges = async () => {
  const list: CoinAPI_Symbol[] = await get("/v1/symbols", {
    params: {
      filter_symbol_id: "SPOT"
    }
  });
  if (typeof list === "string") {
    throw new Error("/v1/symbols payload is invalid! Got a string!");
  }

  const pairExchanges = [];
  for (const item of list) {
    const pairExchange = symbolToPairExchange(item.symbol_id);
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

const subscribePriceUpdate = () =>
  Observable.create(o => {
    log("coinapi WebSocket: create (total=" + websocketTotal + ")");
    let done = false;
    if (websocketTotal >= MAX_WEBSOCKET) {
      log(
        "coinapi WebSocket: too many WebSocket opened. this should not happen"
      );
      process.exit(1);
    }
    websocketTotal++;
    const ws = new WebSocket("wss://ws.coinapi.io/v1/");
    const tickers = allTickers;
    ws.on("open", () => {
      if (done) {
        try {
          ws.close();
        } catch (e) {
          logError("failed to close WebSocket", e);
        }
        return;
      }
      log("coinapi WebSocket: open");
      ws.send(
        JSON.stringify({
          type: "hello",
          apikey: COINAPI_KEY,
          heartbeat: false,
          subscribe_data_type: ["trade"],
          subscribe_filter_asset_id: tickers
        })
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
          (r: CoinAPI_TickerMessage);
          const maybePairExchange = symbolToPairExchange(r.symbol_id);
          if (maybePairExchange) {
            o.next({
              pairExchangeId: maybePairExchange.id,
              price: r.price
            });
          }
        }
      }
    });
    ws.on("close", () => {
      log("coinapi WebSocket: close");
      done = true;
      o.complete();
      websocketTotal--;
    });

    function unsubscribe() {
      log("coinapi WebSocket: unsubscribe");
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
