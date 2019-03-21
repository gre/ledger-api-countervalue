// @flow
// cache on top of coinapi & db

import { of } from "rxjs/observable/of";
import { bufferTime } from "rxjs/operators";
import { fromPromise } from "rxjs/observable/fromPromise";
import { mergeMap } from "rxjs/operators";

import type {
  PriceUpdate,
  Histo,
  Pair,
  HistoAPIResponse,
  ExchangesAPIRequest,
  ExchangesAPIResponse,
  RequestPair,
  Granularity,
  PairExchange,
  DB_PairExchangeData
} from "./types";
import { getCurrentDatabase } from "./db";
import { getCurrentProvider } from "./providers";
import { tickersByMarketcap } from "./coinmarketcap";
import {
  formatTime,
  pairExchangeFromId,
  getFiatOrCurrency,
  convertToCentSatRate,
  supportTicker,
  promiseThrottle,
  granularityMs
} from "./utils";
import {
  failRefreshingData,
  failSyncStats,
  pullLiveRatesDebugMessage,
  log
} from "./logger";

const provider = getCurrentProvider();
provider.init();

const db = getCurrentDatabase();

const throttles = {
  fetchPairExchanges: 60 * 60 * 1000,
  fetchExchanges: 60 * 60 * 1000,
  fetchHisto: 15 * 60 * 1000,
  updateLiveRates: 1000
};

// TODO all promiseThrottle should use a DB so it is shared across multiple server instances
// also I think this cache need to exist at the provider level.

const fetchAndCacheAvailablePairExchanges = promiseThrottle(async () => {
  const { fetchAvailablePairExchanges } = provider;
  const pairExchanges: PairExchange[] = await fetchAvailablePairExchanges();
  const pairExchangesData: DB_PairExchangeData[] = pairExchanges.map(s => ({
    from: s.from,
    to: s.to,
    from_to: s.from + "_" + s.to,
    exchange: s.exchange,
    id: s.id,
    latest: 0,
    latestDate: null,
    yesterdayVolume: 0,
    oldestDayAgo: 0,
    hasHistoryFor1Year: false,
    hasHistoryFor30LastDays: true, // optimistically thinking the pairExchange will have data, updates at each sync
    historyLoadedAt_daily: null,
    historyLoadedAt_hourly: null,
    histo_daily: {},
    histo_hourly: {}
  }));
  await db.insertPairExchangeData(pairExchangesData);
  return pairExchangesData;
}, throttles.fetchPairExchanges);

const fetchAndCacheAllExchanges = promiseThrottle(async () => {
  const { fetchExchanges } = provider;
  const exchanges = await fetchExchanges();
  await db.updateExchanges(exchanges);
  return exchanges;
}, throttles.fetchExchanges);

const MINIMAL_DAYS_TO_CONSIDER_EXCHANGE = Math.min(
  process.env.MINIMAL_DAYS_TO_CONSIDER_EXCHANGE
    ? parseInt(process.env.MINIMAL_DAYS_TO_CONSIDER_EXCHANGE, 10)
    : 20,
  30
);

// a high order of volatility is extreme and tells something is wrong with data
const MAXIMUM_RATIO_EXTREME_VARIATION = 1000;

const syncPairStats = async (
  pairExchangeId: string,
  histoDays: Histo,
  stats: Object = {}
) => {
  const now = new Date();
  const nowT = Date.now();

  const pairExchangeData = pairExchangeFromId(pairExchangeId);
  if (!pairExchangeData) return;
  const from = getFiatOrCurrency(pairExchangeData.from);
  const to = getFiatOrCurrency(pairExchangeData.to);
  if (!from || !to) return;

  let oldestDayAgo = 0;

  const granMs = granularityMs.daily;

  const days = Object.keys(histoDays)
    .filter(k => k !== "latest")
    .map(k => new Date(k).getTime());

  if (days.length === 0) {
    // in this case, we do nothing because there is probably no data yet!
    return;
  }

  oldestDayAgo = Math.floor((now - Math.min(...days)) / granMs);

  let minimum = histoDays.latest || Infinity;
  let maximum = histoDays.latest || 0;
  let historyCount: number = "latest" in histoDays ? 1 : 0;
  for (let t = nowT - 30 * granMs; t < nowT - granMs; t += granMs) {
    const key = formatTime(new Date(t), "daily");
    const value = histoDays[key];
    if (key in histoDays && value > 0) {
      historyCount++;
      minimum = Math.min(minimum, histoDays[key]);
      maximum = Math.max(maximum, histoDays[key]);
    }
  }

  const minMaxRatio = maximum / minimum;
  const invalidRatio =
    minMaxRatio <= 0 || !isFinite(minMaxRatio) || isNaN(minMaxRatio);

  if (!invalidRatio && minMaxRatio >= MAXIMUM_RATIO_EXTREME_VARIATION) {
    log("ExtremeRatioFound: " + minMaxRatio + " on " + pairExchangeId);
  }

  // We assume an exchange valid if it have enough days data AND there is no invalid datapoints
  const hasHistoryFor30LastDays =
    historyCount >= MINIMAL_DAYS_TO_CONSIDER_EXCHANGE &&
    !invalidRatio &&
    minMaxRatio < MAXIMUM_RATIO_EXTREME_VARIATION;

  const hasHistoryFor1Year = oldestDayAgo > 365;

  stats.oldestDayAgo = oldestDayAgo;
  stats.hasHistoryFor30LastDays = hasHistoryFor30LastDays;
  stats.hasHistoryFor1Year = hasHistoryFor1Year;

  await db.updatePairExchangeStats(pairExchangeId, stats);
};

const fetchHisto = async (
  pairExchangeId: string,
  granularity: Granularity
): Histo => {
  const { fetchHistoSeries } = provider;
  const now = new Date();
  const nowT = Date.now();
  const histo: Histo = {};
  const history = await fetchHistoSeries(pairExchangeId, granularity);
  history.sort((a, b) => b.time - a.time);
  const pairExchangeData = pairExchangeFromId(pairExchangeId);
  if (!pairExchangeData) return histo;
  const from = getFiatOrCurrency(pairExchangeData.from);
  const to = getFiatOrCurrency(pairExchangeData.to);
  if (!from || !to) return histo;

  let oldestDayAgo = 0;

  const granMs = granularityMs[granularity];
  for (const data of history) {
    const key =
      data.time > now - granMs ? "latest" : formatTime(data.time, granularity);
    oldestDayAgo = Math.max(
      Math.floor((now - data.time) / granMs),
      oldestDayAgo
    );
    const rate = convertToCentSatRate(from, to, data.close);
    histo[key] = rate;
  }

  const stats: Object = {
    [`historyLoadedAt_${granularity}`]: formatTime(now, granularity)
  };

  if (granularity === "daily") {
    const yesterdayVolume =
      history[1] && new Date(history[1].time) > new Date(nowT - 2 * granMs)
        ? history[1].volume
        : 0;
    stats.yesterdayVolume = yesterdayVolume;
    if (histo.latest) {
      stats.latestDate = now;
    }

    syncPairStats(pairExchangeId, histo, stats);
  }
  return histo;
};

const fetchDailyMarketCapCoins = promiseThrottle(async () => {
  const now = new Date();
  const day = formatTime(now, "daily");
  let coins = await db.queryMarketCapCoinsForDay(day);
  if (coins) return coins;
  coins = await tickersByMarketcap();
  db.updateMarketCapCoins(day, coins);
  return coins;
}, 60000);

const fetchAndCacheHisto_caches = {};
const fetchAndCacheHisto_makeThrottle = (
  id: string,
  granularity: Granularity
) =>
  promiseThrottle(async () => {
    const now = new Date();
    const nowKey = formatTime(now, granularity);
    const pairExchange = await db.queryPairExchangeById(id);
    if (pairExchange) {
      if (pairExchange[`historyLoadedAt_${granularity}`] === nowKey) {
        // already loaded today
        return pairExchange[`histo_${granularity}`];
      }
      try {
        const history = await fetchHisto(id, granularity);
        db.updateHisto(id, granularity, history);
        return history;
      } catch (e) {
        failRefreshingData(e, "fetchAndCacheHisto");
        return pairExchange[`histo_${granularity}`];
      }
    }
  }, throttles.fetchHisto);

const fetchAndCacheHisto = (id: string, granularity: Granularity) => {
  const key = id + "_" + granularity;
  const f =
    fetchAndCacheHisto_caches[key] ||
    (fetchAndCacheHisto_caches[key] = fetchAndCacheHisto_makeThrottle(
      id,
      granularity
    ));
  return f();
};

const blacklist: string[] = (process.env.BLACKLIST_EXCHANGES || "")
  .toLowerCase()
  .split(",");

const isAcceptedExchange = (exchangeId: string) =>
  !blacklist.includes(exchangeId.toLowerCase());

const filterPairExchanges = all =>
  all.filter(o => isAcceptedExchange(o.exchange));

export const getPairExchangesForPairs = async (pairs: Pair[]) => {
  try {
    await fetchAndCacheAvailablePairExchanges();
  } catch (e) {
    failRefreshingData(e, "getPairExchangesForPairs");
  }
  const pairExchanges = await db.queryPairExchangesByPairs(pairs);
  return filterPairExchanges(pairExchanges);
};

export const getPairExchangesForPair = async (pair: Pair, opts: *) => {
  try {
    await fetchAndCacheAvailablePairExchanges();
  } catch (e) {
    failRefreshingData(e, "getPairExchangesForPair");
  }
  const pairExchanges = await db.queryPairExchangesByPair(pair, opts);
  return filterPairExchanges(pairExchanges);
};

export async function getHistoRequest(
  pairs: RequestPair[],
  granularity: Granularity
): Promise<HistoAPIResponse> {
  const response = {};
  const pairExchanges = await getPairExchangesForPairs(
    pairs.map(({ from, to }) => ({ from, to }))
  );
  for (const { from, to, exchange, after, at } of pairs) {
    const pairExchangeCandidates = pairExchanges.filter(
      s => s.from === from && s.to === to && s.hasHistoryFor30LastDays
    );
    const pairExchange = exchange
      ? pairExchangeCandidates.find(s => s.exchange === exchange)
      : pairExchangeCandidates[0];
    if (pairExchange) {
      const histo = await fetchAndCacheHisto(pairExchange.id, granularity);
      const pairResult = {};
      for (let key in histo) {
        if (at ? at.includes(key) : !after || key > after) {
          pairResult[key] = histo[key];
        }
      }
      pairResult.latest = pairExchange.latest;
      if (!response[to]) response[to] = {};
      if (!response[to][from]) response[to][from] = {};
      response[to][from][pairExchange.exchange] = pairResult;
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
    exchanges = await db.queryExchanges();
  }
  const pairExchanges = await getPairExchangesForPair(request.pair, {
    filterWithHistory: true
  });
  return pairExchanges.map(s => {
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

export const getDailyMarketCapCoins = async () => fetchDailyMarketCapCoins();

const $availablePairExchanges = of(null).pipe(
  mergeMap(() => fromPromise(fetchAndCacheAvailablePairExchanges()))
);

const $priceUpdates = $availablePairExchanges.pipe(
  mergeMap(provider.subscribePriceUpdate)
);

const $bufferedPriceUpdates = $priceUpdates.pipe(
  bufferTime(throttles.updateLiveRates)
);

export const pullLiveRates = (
  onError: (?Error) => void,
  onComplete: () => void
) =>
  $bufferedPriceUpdates.subscribe(
    (buf: PriceUpdate[]) => {
      if (buf.length === 0) return;
      const ratesPerId: { [_: string]: number } = {};
      for (const msg of buf) {
        const pe = pairExchangeFromId(msg.pairExchangeId);
        if (supportTicker(pe.from) && supportTicker(pe.to)) {
          const from = getFiatOrCurrency(pe.from);
          const to = getFiatOrCurrency(pe.to);
          ratesPerId[pe.id] = convertToCentSatRate(from, to, msg.price);
        }
      }
      const liveRates = Object.keys(ratesPerId).map(pairExchangeId => ({
        pairExchangeId,
        price: ratesPerId[pairExchangeId]
      }));
      if (process.env.DEBUG_LIVE_RATES) {
        pullLiveRatesDebugMessage(liveRates);
      }
      db.updateLiveRates(liveRates);
    },
    onError,
    onComplete
  );

const delay = ms => new Promise(success => setTimeout(success, ms));

export const prefetchAllPairExchanges = async () => {
  try {
    const pairExchanges = await fetchAndCacheAvailablePairExchanges();

    const prioritizePairExchange = ({ latestDate }) =>
      -(latestDate ? Number(latestDate) : 0);

    const sorted = pairExchanges
      .slice(0)
      .sort((a, b) => prioritizePairExchange(b) - prioritizePairExchange(a));

    for (const pairExchange of sorted) {
      await fetchAndCacheHisto(pairExchange.id, "daily");
      await fetchAndCacheHisto(pairExchange.id, "hourly");
      // general idea is to schedule fetches over the fetch histo throttle so the calls are dispatched over time.
      await delay(throttles.fetchHisto / pairExchanges.length);
    }
  } catch (e) {
    failRefreshingData(e, "all pairExchanges");
  }
};

export const syncAllPairExchangeStats = async () => {
  try {
    const pairExchanges = await db.queryPairExchangeIds();
    for (const id of pairExchanges) {
      const { histo_daily } = await db.queryPairExchangeById(id, {
        histo_daily: 1
      });
      await syncPairStats(id, histo_daily);
    }
  } catch (e) {
    failSyncStats(e);
  }
};
