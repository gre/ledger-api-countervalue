// @flow

import axios from "axios";
import { cryptoTickers } from "./utils";
import { logAPI, logAPIError } from "./logger";

const CMC_API_KEY = process.env.CMC_API_KEY;
if (!CMC_API_KEY) {
  throw new Error("CMC_API_KEY env is required. https://pro.coinmarketcap.com");
}

const get = async (url: string, opts?: *) => {
  const beforeTime = Date.now();
  try {
    const res = await axios.get(`https://pro-api.coinmarketcap.com${url}`, {
      timeout: 60000,
      headers: {
        "X-CMC_PRO_API_KEY": CMC_API_KEY
      },
      ...opts
    });
    logAPI({
      api: "CMC",
      url,
      opts,
      duration: Date.now() - beforeTime,
      status: res.status
    });
    return res.data;
  } catch (error) {
    logAPIError({
      api: "CMC",
      error,
      url,
      opts,
      duration: Date.now() - beforeTime
    });
    throw error;
  }
};

export async function tickersByMarketcap(): Promise<string[]> {
  const r = await get("/v1/cryptocurrency/listings/latest", {
    params: {
      limit: 5000
    }
  });
  return r.data
    .map(c => c.symbol)
    .filter(ticker => cryptoTickers.includes(ticker));
}
