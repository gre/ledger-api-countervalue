//@flow

import WebSocket from "ws";
import axios from "axios";
import { supportTicker } from "./utils";
import { logAPI, logAPIError } from "./logger";
import type {
  CoinAPI_Exchange,
  CoinAPI_TickerMessage,
  CoinAPI_Symbol,
  CoinAPI_Timeseries
} from "./types";

const url = "wss://ws.coinapi.io/v1/";

type Subscription = { unsubscribe: () => void };

const COINAPI_KEY = process.env.COINAPI_KEY;

if (!COINAPI_KEY) throw new Error("COINAPI_KEY env is not defined");

export const get = async (url: string, opts?: *) => {
  const beforeTime = Date.now();
  try {
    const res = await axios.get(`https://rest.coinapi.io${url}`, {
      ...opts,
      timeout: 30000,
      headers: {
        "X-CoinAPI-Key": COINAPI_KEY
      }
    });
    logAPI({
      url,
      opts,
      duration: Date.now() - beforeTime,
      status: res.status
    });
    return res.data;
  } catch (error) {
    logAPIError({ error, url, duration: Date.now() - beforeTime });
    throw error;
  }
};

export const fetchHistodaysSeries = async (
  symbol: string,
  limit: number = 3560
) => {
  const days: CoinAPI_Timeseries[] = await get(`/v1/ohlcv/${symbol}/latest`, {
    params: {
      period_id: "1DAY",
      limit
    }
  });
  return days;
};

export const fetchCalculatedCurrentRates = async (ticker: string) => {
  const res: {
    rates: Array<{
      time: string,
      asset_id_quote: string,
      rate: number
    }>
  } = await get(`/v1/exchangerate/${ticker}`);
  return res.rates;
};

export const fetchExchanges = async () => {
  const list: CoinAPI_Exchange[] = await get("/v1/exchanges");
  return list;
};

export const fetchAvailableSpotSymbols = async () => {
  const list: CoinAPI_Symbol[] = await get("/v1/symbols", {
    params: {
      filter_symbol_id: "SPOT"
    }
  });
  return list.filter(
    item =>
      item.symbol_type === "SPOT" &&
      supportTicker(item.asset_id_base) &&
      supportTicker(item.asset_id_quote)
  );
};

export const subscribeTickerMessages = (
  tickers: string[],
  onMessage: CoinAPI_TickerMessage => void,
  onError: string => void,
  onComplete: () => void
): Subscription => {
  const ws = new WebSocket(url);
  ws.on("open", () => {
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
    const r = JSON.parse(data);
    if (r && typeof r === "object") {
      if (r.type === "error") {
        onError(r.message);
        ws.close();
      } else {
        onMessage(r);
      }
    }
  });
  ws.on("close", () => {
    onComplete();
  });

  function unsubscribe() {
    ws.close();
  }

  return { unsubscribe };
};
