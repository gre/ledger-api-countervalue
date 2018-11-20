// @flow

import type { Observable } from "rxjs";

// Generic types

export type Histodays = {
  [day: string]: number
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
  afterDay?: ?string
|};

export type DailyAPIRequest = {|
  pairs: Array<RequestPair>
|};

export type DailyAPIResponsePairData = {
  [day: string]: number,
  latest: number
};

export type DailyAPIResponse = {
  [toTicker: string]: {
    [fromTicker: string]: {
      [exchange: string]: DailyAPIResponsePairData
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
  fetchHistodaysSeries: (
    pairExchangeId: string,
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

  updateHistodays: (
    pairExchangeId: string,
    histodays: Histodays
  ) => Promise<void>,

  updateExchanges: (DB_Exchange[]) => Promise<void>,

  insertPairExchangeData: (DB_PairExchangeData[]) => Promise<void>,

  updatePairExchangeStats: (
    pairExchangeId: string,
    stats: {
      yesterdayVolume?: number,
      hasHistoryFor30LastDays?: boolean,
      historyLoadedAtDay?: string,
      latestDate?: Date
    }
  ) => Promise<void>,

  updateMarketCapCoins: (
    day: string,
    markercapcoins: string[]
  ) => Promise<void>,

  queryExchanges: () => Promise<DB_Exchange[]>,

  queryPairExchangesByPairs: (pairs: Pair[]) => Promise<DB_PairExchangeData[]>,

  queryPairExchangesByPair: (
    pair: Pair,
    opts?: { filterWithHistory?: boolean }
  ) => Promise<DB_PairExchangeData[]>,

  queryPairExchangeById: (
    pairExchangeId: string
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
  histodays: Histodays, // historic by day of rates. the last day is yesterday
  latest: number, // latest rate loaded (usually updated by websocket live connection)

  // some derivated stats:
  latestDate: ?Date,
  yesterdayVolume: number, // the volume that was traded yesterday
  hasHistoryFor30LastDays: boolean, // track if the histodays are available for the last 30 days
  historyLoadedAtDay: ?string // YYYY-MM-DD date where the histodays was loaded
|};
