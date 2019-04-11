// @flow

import type { Observable } from "rxjs";

export type Granularity = "daily" | "hourly";

// Generic types

export type Histo = {
  [_: string]: number
};

export type Pair = {|
  from: string,
  to: string
|};

export type PairExchange = {|
  id: string, // EXCHANGE_FROM_TO format
  exchange: string,
  from: string,
  to: string
|};

export type PriceUpdate = {|
  pairExchangeId: string,
  price: number
|};

export type Exchange = {|
  id: string,
  name: string,
  website: ?string
|};

export type TimeseriesOHLCVR = {|
  time: Date,
  open: number,
  high: number,
  low: number,
  close: number,
  volume: number
|};

// API types

export type RequestPair = {|
  from: string,
  to: string,
  exchange?: ?string,
  after?: ?string,
  at?: string[]
|};

export type HistoAPIRequest = {|
  pairs: Array<RequestPair>,
  granularity: Granularity
|};

export type HistoAPIResponsePairData = {
  [_: string]: number,
  latest: number
};

export type HistoAPIResponse = {
  [toTicker: string]: {
    [fromTicker: string]: {
      [exchange: string]: HistoAPIResponsePairData
    }
  }
};

export type ExchangesAPIRequest = {|
  pair: Pair
|};

export type ExchangesAPIResponse = Array<Exchange>;

// Provider types

export type Provider = {
  init: () => void,
  fetchHistoSeries: (
    pairExchangeId: string,
    granularity: Granularity,
    limit?: number
  ) => Promise<TimeseriesOHLCVR[]>,
  fetchExchanges: () => Promise<Exchange[]>,
  fetchAvailablePairExchanges: () => Promise<PairExchange[]>,
  subscribePriceUpdate: (
    pairExchanges: PairExchange[]
  ) => Observable<PriceUpdate>
};
// Database types

export type DBMeta = {
  lastLiveRatesSync: Date,
  lastMarketCapSync: Date
};

export type Database = {
  init: () => Promise<void>,

  getMeta: () => Promise<DBMeta>,

  statusDB: () => Promise<void>,

  updateLiveRates: (PriceUpdate[]) => Promise<void>,

  updateHisto: (
    pairExchangeId: string,
    granularity: Granularity,
    histo: Histo,
    options?: {
      setLatest?: {
        latest: number,
        latestDate: Date
      }
    }
  ) => Promise<void>,

  updateExchanges: (DB_Exchange[]) => Promise<void>,

  insertPairExchangeData: (DB_PairExchangeData[]) => Promise<void>,

  updatePairExchangeStats: (
    pairExchangeId: string,
    stats: {
      yesterdayVolume?: number,
      hasHistoryFor1Year?: boolean,
      oldestDayAgo?: number,
      hasHistoryFor30LastDays?: boolean,
      historyLoadedAt_day?: string,
      historyLoadedAt_hour?: string,
      latestDate?: Date
    }
  ) => Promise<void>,

  updateMarketCapCoins: (
    day: string,
    markercapcoins: string[]
  ) => Promise<void>,

  queryExchanges: () => Promise<DB_Exchange[]>,

  queryPairExchangeIds: () => Promise<string[]>,

  queryPairExchangesByPairs: (
    pairs: Pair[],
    opts?: {
      granularity?: Granularity,
      withoutRates?: boolean
    }
  ) => Promise<DB_PairExchangeData[]>,

  queryPairExchangesByPair: (
    pair: Pair,
    opts?: {
      filterWithHistory?: boolean,
      granularity?: Granularity,
      withoutRates?: boolean
    }
  ) => Promise<DB_PairExchangeData[]>,

  queryPairExchangeById: (
    pairExchangeId: string,
    project?: Object
  ) => Promise<DB_PairExchangeData>,

  queryMarketCapCoinsForDay: (day: string) => Promise<?(string[])>
};

export type DB_Exchange = {|
  id: string,
  name: string,
  website: ?string
|};

export type DB_PairExchangeData = {|
  id: string,
  from: string, // e.g BTC
  to: string, // e.g. USD
  from_to: string,
  exchange: string, // e.g. KRAKEN. id in DB_Exchange

  histo_daily: Histo, // historic by day of rates. (i.e. the last day is yesterday)
  histo_hourly: Histo, // historic by hour
  latest: number, // live value of latest rate loaded (usually updated by websocket live connection)

  // some derivated stats:
  latestDate: ?Date,
  yesterdayVolume: number, // the volume that was traded yesterday
  oldestDayAgo: number,
  hasHistoryFor1Year: boolean,
  hasHistoryFor30LastDays: boolean, // track if the histo for days are available for the last 30 days
  historyLoadedAt_daily?: ?string,
  historyLoadedAt_hourly?: ?string
|};
