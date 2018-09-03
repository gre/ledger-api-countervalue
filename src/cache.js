// @flow
// cache on top of coinapi & db

import { of } from "rxjs/observable/of";
import { bufferTime } from "rxjs/operators";
import { fromPromise } from "rxjs/observable/fromPromise";
import { mergeMap } from "rxjs/operators";

import type {
  PriceUpdate,
  Histodays,
  Pair,
  DailyAPIResponse,
  ExchangesAPIRequest,
  ExchangesAPIResponse,
  RequestPair
} from "./types";
import { getCurrentDatabase } from "./db";
import { getCurrentProvider } from "./providers";
import { tickersByMarketcap } from "./coinmarketcap";
import {
  formatDay,
  pairExchangeFromId,
  getFiatOrCurrency,
  convertToCentSatRate,
  supportTicker,
  promiseThrottle
} from "./utils";
import { failRefreshingData, pullLiveRatesDebugMessage } from "./logger";

const provider = getCurrentProvider();
provider.init();

const db = getCurrentDatabase();

const throttles = {
  fetchPairExchanges: 60 * 60 * 1000,
  fetchExchanges: 60 * 60 * 1000,
  fetchHistodays: 15 * 60 * 1000,
  updateLiveRates: 1000
};

// TODO all promiseThrottle should use a DB so it is shared across multiple server instances
// also I think this cache need to exist at the provider level.

const fetchAndCacheAvailablePairExchanges = promiseThrottle(async () => {
  const { fetchAvailablePairExchanges } = provider;
  const pairExchanges = await fetchAvailablePairExchanges();
  const pairExchangesData = pairExchanges.map(s => ({
    from: s.from,
    to: s.to,
    from_to: s.from + "_" + s.to,
    exchange: s.exchange,
    id: s.id,
    latest: 0,
    latestDate: null,
    yesterdayVolume: 0,
    hasHistoryFor30LastDays: true, // optimistically thinking the pairExchange will have data, updates at each sync
    historyLoadedAtDay: null,
    histodays: {}
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

const DAY = 24 * 60 * 60 * 1000;

const MINIMAL_DAYS_TO_CONSIDER_EXCHANGE = Math.min(
  process.env.MINIMAL_DAYS_TO_CONSIDER_EXCHANGE
    ? parseInt(process.env.MINIMAL_DAYS_TO_CONSIDER_EXCHANGE, 10)
    : 20,
  30
);

const fetchHistodays = async (pairExchangeId: string): Histodays => {
  const { fetchHistodaysSeries } = provider;
  const now = new Date();
  const nowT = Date.now();
  const histodays: Histodays = {};
  const history = await fetchHistodaysSeries(pairExchangeId);
  history.sort((a, b) => b.time - a.time);
  const pairExchangeData = pairExchangeFromId(pairExchangeId);
  if (!pairExchangeData) return histodays;
  const from = getFiatOrCurrency(pairExchangeData.from);
  const to = getFiatOrCurrency(pairExchangeData.to);
  if (!from || !to) return histodays;
  for (const data of history) {
    const day = data.time > now - DAY ? "latest" : formatDay(data.time);
    const rate = convertToCentSatRate(from, to, data.close);
    histodays[day] = rate;
  }
  const yesterdayVolume =
    history[1] && new Date(history[1].time) > new Date(nowT - 2 * DAY)
      ? history[1].volume
      : 0;

  let historyCount: number = "latest" in histodays ? 1 : 0;
  for (let t = nowT - 30 * DAY; t < nowT - DAY; t += DAY) {
    if (formatDay(new Date(t)) in histodays) {
      historyCount++;
    }
  }

  const hasHistoryFor30LastDays =
    historyCount >= MINIMAL_DAYS_TO_CONSIDER_EXCHANGE;
  const stats: Object = {
    yesterdayVolume,
    hasHistoryFor30LastDays,
    historyLoadedAtDay: formatDay(now)
  };
  if (histodays.latest) {
    stats.latestDate = now;
  }
  db.updatePairExchangeStats(pairExchangeId, stats);
  return histodays;
};

const fetchDailyMarketCapCoins = promiseThrottle(async () => {
  const now = new Date();
  const day = formatDay(now);
  let coins = await db.queryMarketCapCoinsForDay(day);
  if (coins) return coins;
  coins = await tickersByMarketcap();
  db.updateMarketCapCoins(day, coins);
  return coins;
}, 60000);

const fetchAndCacheHistodays_caches = {};
const fetchAndCacheHistodays_makeThrottle = (id: string) =>
  promiseThrottle(async () => {
    const now = new Date();
    const day = formatDay(now);
    const pairExchange = await db.queryPairExchangeById(id);
    if (pairExchange) {
      if (pairExchange.historyLoadedAtDay === day) {
        // already loaded today
        return pairExchange.histodays;
      }
      try {
        const histodays = await fetchHistodays(id);
        db.updateHistodays(id, histodays);
        return histodays;
      } catch (e) {
        failRefreshingData(e, "fetchAndCacheHistodays");
        return pairExchange.histodays;
      }
    }
  }, throttles.fetchHistodays);

const fetchAndCacheHistodays = (id: string) => {
  const f =
    fetchAndCacheHistodays_caches[id] ||
    (fetchAndCacheHistodays_caches[id] = fetchAndCacheHistodays_makeThrottle(
      id
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

export async function getDailyRequest(
  pairs: RequestPair[]
): Promise<DailyAPIResponse> {
  const response = {};
  const pairExchanges = await getPairExchangesForPairs(
    pairs.map(({ from, to }) => ({ from, to }))
  );
  for (const { from, to, exchange, afterDay } of pairs) {
    const pairExchangeCandidates = pairExchanges.filter(
      s => s.from === from && s.to === to && s.hasHistoryFor30LastDays
    );
    const pairExchange = exchange
      ? pairExchangeCandidates.find(s => s.exchange === exchange)
      : pairExchangeCandidates[0];
    if (pairExchange) {
      const histodays = await fetchAndCacheHistodays(pairExchange.id);
      const pairResult = {};
      for (let day in histodays) {
        if (!afterDay || day > afterDay) {
          pairResult[day] = histodays[day];
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
      await fetchAndCacheHistodays(pairExchange.id);
      // general idea is to schedule fetches over the fetch histodays throttle so the calls are dispatched over time.
      await delay(throttles.fetchHistodays / pairExchanges.length);
    }
  } catch (e) {
    failRefreshingData(e, "all pairExchanges");
  }
};
