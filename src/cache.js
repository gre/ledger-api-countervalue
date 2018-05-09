// @flow
// cache on top of coinapi & db

import throttle from "lodash/throttle";

import type {
  Histodays,
  Pair,
  DailyAPIResponse,
  DailyAPIResponsePairData,
  ExchangesAPIRequest,
  ExchangesAPIResponse,
  RequestPair,
  CoinAPI_TickerMessage
} from "./types";
import {
  updateHistodays,
  queryExchanges,
  querySymbolsByPairs,
  querySymbolsByPair,
  querySymbolById,
  updateSymbolStats,
  updateExchanges,
  updateAssetSymbols,
  updateLiveRates
} from "./db";
import {
  fetchAvailableSpotSymbols,
  fetchExchanges,
  fetchHistodaysSeries,
  subscribeTickerMessages
} from "./coinapi";
import {
  formatDay,
  parseSymbol,
  getFiatOrCurrency,
  convertToCentSatRate,
  allTickers,
  supportTicker,
  promiseThrottle
} from "./utils";
import { failRefreshingData } from "./logger";

const throttles = {
  fetchSymbols: 60 * 60 * 1000,
  fetchExchanges: 60 * 60 * 1000,
  fetchHistodays: 15 * 60 * 1000,
  updateLiveRates: 1000
};

const fetchAndCacheAvailableSpotSymbols = promiseThrottle(async () => {
  const spotSymbols = await fetchAvailableSpotSymbols();
  const symbols = spotSymbols.map(s => ({
    from: s.asset_id_base,
    to: s.asset_id_quote,
    from_to: s.asset_id_base + "_" + s.asset_id_quote,
    exchange: s.exchange_id,
    id: s.symbol_id,
    latest: 0,
    latestDate: null,
    yesterdayVolume: 0,
    hasHistoryFor30LastDays: true, // optimistically thinking the symbol will have data, updates at each sync
    historyLoadedAtDay: null,
    histodays: {}
  }));
  await updateAssetSymbols(symbols);
  return symbols;
}, throttles.fetchSymbols);

const fetchAndCacheAllExchanges = promiseThrottle(async () => {
  const apiExchanges = await fetchExchanges();
  const exchanges = apiExchanges.map(e => ({
    id: e.exchange_id,
    website: e.website,
    name: e.name
  }));
  await updateExchanges(exchanges);
  return exchanges;
}, throttles.fetchExchanges);

const DAY = 24 * 60 * 60 * 1000;

const fetchHistodays = async (symbol: string): Histodays => {
  const now = new Date();
  const nowT = Date.now();
  const histodays: Histodays = {};
  const history = await fetchHistodaysSeries(symbol);
  const symbolData = parseSymbol(symbol);
  if (!symbolData) return histodays;
  const from = getFiatOrCurrency(symbolData.from);
  const to = getFiatOrCurrency(symbolData.to);
  if (!from || !to) return histodays;
  for (const data of history) {
    const day =
      new Date(data.time_period_end) > now
        ? "latest"
        : formatDay(new Date(data.time_period_start));
    const rate = convertToCentSatRate(from, to, data.price_close);
    histodays[day] = rate;
  }
  const yesterdayVolume =
    history[1] && new Date(history[1].time_period_end) > new Date(nowT - DAY)
      ? history[1].volume_traded
      : 0;
  let hasHistoryFor30LastDays = "latest" in histodays;
  if (hasHistoryFor30LastDays) {
    for (let t = nowT - 30 * DAY; t < nowT - DAY; t += DAY) {
      if (!(formatDay(new Date(t)) in histodays)) {
        hasHistoryFor30LastDays = false;
        break;
      }
    }
  }
  const stats: Object = {
    yesterdayVolume,
    hasHistoryFor30LastDays,
    historyLoadedAtDay: formatDay(now)
  };
  if (histodays.latest) {
    stats.latestDate = now;
  }
  updateSymbolStats(symbol, stats);
  return histodays;
};

const fetchAndCacheHistodays_caches = {};
const fetchAndCacheHistodays_makeThrottle = (symbolId: string) =>
  promiseThrottle(async () => {
    const now = new Date();
    const day = formatDay(now);
    const symbol = await querySymbolById(symbolId);
    if (symbol) {
      if (symbol.historyLoadedAtDay === day) {
        // already loaded today
        return symbol.histodays;
      }
      try {
        const histodays = await fetchHistodays(symbolId);
        updateHistodays(symbolId, histodays);
        return histodays;
      } catch (e) {
        return symbol.histodays;
      }
    }
  }, throttles.fetchHistodays);

const fetchAndCacheHistodays = (symbolId: string) => {
  const f =
    fetchAndCacheHistodays_caches[symbolId] ||
    (fetchAndCacheHistodays_caches[
      symbolId
    ] = fetchAndCacheHistodays_makeThrottle(symbolId));
  return f();
};

export const getSymbolsForPairs = async (pairs: Pair[]) => {
  try {
    await fetchAndCacheAvailableSpotSymbols();
  } catch (e) {
    failRefreshingData(e, "symbols");
  }
  const symbols = await querySymbolsByPairs(pairs);
  return symbols;
};

export const getSymbolsForPair = async (pair: Pair, opts: *) => {
  try {
    await fetchAndCacheAvailableSpotSymbols();
  } catch (e) {
    failRefreshingData(e, "symbols");
  }
  const symbols = await querySymbolsByPair(pair, opts);
  return symbols;
};

export async function getDailyRequest(
  pairs: RequestPair[]
): Promise<DailyAPIResponse> {
  const response = {};
  const symbols = await getSymbolsForPairs(
    pairs.map(({ from, to }) => ({ from, to }))
  );
  for (const { from, to, exchange, afterDay } of pairs) {
    const symbolCandidates = symbols.filter(
      s => s.from === from && s.to === to
    );
    const symbol = exchange
      ? symbolCandidates.find(s => s.exchange === exchange)
      : symbolCandidates[0];
    if (symbol) {
      const histodays = await fetchAndCacheHistodays(symbol.id);
      const pairResult: DailyAPIResponsePairData = {
        latest: symbol.latest
      };
      for (let day in histodays) {
        if (!afterDay || day > afterDay) {
          pairResult[day] = histodays[day];
        }
      }
      if (!response[to]) response[to] = {};
      if (!response[to][from]) response[to][from] = {};
      response[to][from][symbol.exchange] = pairResult;
    }
  }
  return response;
}

export const getExchanges = async (
  request: ExchangesAPIRequest
): Promise<ExchangesAPIResponse> => {
  let exchanges;
  try {
    exchanges = await fetchAndCacheAllExchanges();
  } catch (e) {
    failRefreshingData(e, "exchanges");
    exchanges = await queryExchanges();
  }
  const symbols = await getSymbolsForPair(request.pair, {
    filterWithHistory: true
  });
  return symbols.map(s => {
    const { id, name, website } = exchanges.find(e => e.id === s.exchange) || {
      id: s.exchange,
      name: s.exchange,
      website: null
    };
    return {
      id,
      name,
      website
    };
  });
};

export const pullLiveRates = (
  onMsg: CoinAPI_TickerMessage => void,
  onError: string => void,
  onEnd: () => void = () => {}
) => {
  const tickerMessagesBuffer: CoinAPI_TickerMessage[] = [];
  const flushBuffer = throttle(() => {
    if (tickerMessagesBuffer.length === 0) return;
    const ratesPerSymbol: { [_: string]: number } = {};
    for (const msg of tickerMessagesBuffer) {
      const symbol = msg.symbol_id;
      const symbolData = parseSymbol(symbol);
      if (
        symbolData &&
        supportTicker(symbolData.from) &&
        supportTicker(symbolData.to)
      ) {
        const from = getFiatOrCurrency(symbolData.from);
        const to = getFiatOrCurrency(symbolData.to);
        ratesPerSymbol[symbol] = convertToCentSatRate(from, to, msg.price);
      }
    }
    const liveRates = Object.keys(ratesPerSymbol).map(symbol => ({
      symbol,
      rate: ratesPerSymbol[symbol]
    }));
    updateLiveRates(liveRates);
  }, throttles.updateLiveRates);

  return subscribeTickerMessages(
    allTickers,
    msg => {
      tickerMessagesBuffer.push(msg);
      flushBuffer();
      onMsg(msg);
    },
    onError,
    onEnd
  );
};

const delay = ms => new Promise(success => setTimeout(success, ms));

export const prefetchAllSymbols = async () => {
  try {
    const symbols = await fetchAndCacheAvailableSpotSymbols();
    for (const symbol of symbols) {
      await fetchAndCacheHistodays(symbol.id);
      // general idea is to schedule fetches over the fetch histodays throttle so the calls are dispatched over time.
      await delay(throttles.fetchHistodays / symbols.length);
    }
  } catch (e) {
    failRefreshingData(e, "all symbols");
  }
};
