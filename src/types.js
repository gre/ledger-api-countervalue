// @flow

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

export type Histodays = {
  [day: string]: number
};

export type Pair = {|
  from: string,
  to: string
|};

export type ExchangesAPIRequest = {|
  pair: Pair
|};

export type ExchangesAPIResponse = Array<{|
  id: string,
  name: string,
  website: ?string
|}>;

// Database types

export type DB_Exchange = {|
  id: string,
  website: string,
  name: string
|};

export type DB_Symbol = {|
  id: string, // id of the symbol (coinapi format)
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

// Coin API types

export type CoinAPI_TickerMessage = {|
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

export type CoinAPI_Symbol = {|
  symbol_id: string,
  exchange_id: string,
  symbol_type: string,
  asset_id_base: string,
  asset_id_quote: string
|};

export type CoinAPI_Exchange = {|
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

export type CoinAPI_Timeseries = {|
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
