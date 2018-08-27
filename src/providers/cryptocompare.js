//@flow

import { Observable } from "rxjs";
import io from "socket.io-client";
import axios from "axios";
import { supportTicker, pairExchange, pairExchangeFromId } from "../utils";
import { logAPI, logAPIError } from "../logger";
import type { Provider } from "../types";

function init() {}

const get = async (url: string, opts?: *) => {
  const beforeTime = Date.now();
  try {
    const res = await axios.get(`https://min-api.cryptocompare.com${url}`, {
      ...opts,
      timeout: 30000
    });
    logAPI({
      api: "CryptoCompare",
      url,
      opts,
      duration: Date.now() - beforeTime,
      status: res.status
    });
    return res.data;
  } catch (error) {
    logAPIError({
      api: "CryptoCompare",
      error,
      url,
      opts,
      duration: Date.now() - beforeTime
    });
    throw error;
  }
};

type HistodayResponse = {
  Response: string,
  Type: number,
  Aggregated: number,
  TimeTo: number,
  TimeFrom: number,
  FirstValueInArray: boolean,
  ConversionType: {
    type: string,
    conversionSymbol: string
  },
  Data: Array<{
    time: number,
    close: number,
    high: number,
    low: number,
    open: number,
    volumefrom: number,
    volumeto: number
  }>
};
type AllExchangesResponse = {
  [exchange: string]: {
    [from: string]: string[]
  }
};

const fetchHistodaysSeries = async (id: string, limit: number = 3560) => {
  const { from, to, exchange } = pairExchangeFromId(id);
  const r: HistodayResponse = await get("/data/histoday", {
    params: {
      fsym: from,
      tsym: to,
      e: exchange,
      tryConversion: false,
      limit
    }
  });
  if (r.ConversionType.type !== "direct") {
    throw new Error("CryptoCompare could not find a direct conversion");
  }
  return r.Data.map(({ time, open, high, low, close, volumefrom: volume }) => ({
    time: new Date(time * 1000),
    open,
    high,
    low,
    close,
    volume
  }));
};

const fetchExchanges = async () => {
  const r: AllExchangesResponse = await get("/data/all/exchanges");
  const exchanges = [];
  for (const exchange in r) {
    exchanges.push({
      id: exchange,
      name: exchange,
      website: null
    });
  }
  return exchanges;
};

const fetchAvailablePairExchanges = async () => {
  const r: AllExchangesResponse = await get("/data/all/exchanges");
  const all = [];
  for (const exchange in r) {
    const perFrom = r[exchange];
    for (const from in perFrom) {
      if (!supportTicker(from)) continue;
      const tos = perFrom[from];
      for (const to of tos) {
        if (!supportTicker(to)) continue;
        all.push(pairExchange(exchange, from, to));
      }
    }
  }
  return all;
};

const CURRENT = {};

const subscribePriceUpdate = exchangePairs =>
  Observable.create(o => {
    const socket = io("https://streamer.cryptocompare.com/");

    socket.on("connect", () => {});

    socket.on("connect_error", e => {
      o.error(e);
    });

    socket.on("error", e => {
      o.error(e);
    });

    socket.on("disconnect", () => {
      o.complete();
    });

    socket.on("m", m => {
      if (m.split("~")[0] === "2") {
        const data = CURRENT.unpack(m);
        const { from, to, id } = pairExchange(
          data.MARKET,
          data.FROMSYMBOL,
          data.TOSYMBOL
        );
        if (
          supportTicker(from) &&
          supportTicker(to) &&
          typeof data.PRICE === "number" &&
          data.PRICE
        ) {
          o.next({
            pairExchangeId: id,
            price: data.PRICE
          });
        }
      }
    });

    socket.emit("SubAdd", {
      subs: exchangePairs.map(
        ({ exchange, from, to }) => `2~${exchange}~${from}~${to}`
      )
    });

    return {
      unsubscribe: () => {
        socket.close();
      }
    };
  });

const provider: Provider = {
  init,
  fetchHistodaysSeries,
  fetchExchanges,
  fetchAvailablePairExchanges,
  subscribePriceUpdate
};

export default provider;

/*
current fields mask values always in the last ~
*/

CURRENT.FLAGS = {
  PRICEUP: 0x1, // hex for binary 1
  PRICEDOWN: 0x2, // hex for binary 10
  PRICEUNCHANGED: 0x4, // hex for binary 100
  BIDUP: 0x8, // hex for binary 1000
  BIDDOWN: 0x10, // hex for binary 10000
  BIDUNCHANGED: 0x20, // hex for binary 100000
  OFFERUP: 0x40, // hex for binary 1000000
  OFFERDOWN: 0x80, // hex for binary 10000000
  OFFERUNCHANGED: 0x100, // hex for binary 100000000
  AVGUP: 0x200, // hex for binary 1000000000
  AVGDOWN: 0x400, // hex for binary 10000000000
  AVGUNCHANGED: 0x800 // hex for binary 100000000000
};

CURRENT.FIELDS = {
  TYPE: 0x0, // hex for binary 0, it is a special case of fields that are always there
  MARKET: 0x0, // hex for binary 0, it is a special case of fields that are always there
  FROMSYMBOL: 0x0, // hex for binary 0, it is a special case of fields that are always there
  TOSYMBOL: 0x0, // hex for binary 0, it is a special case of fields that are always there
  FLAGS: 0x0, // hex for binary 0, it is a special case of fields that are always there
  PRICE: 0x1, // hex for binary 1
  BID: 0x2, // hex for binary 10
  OFFER: 0x4, // hex for binary 100
  LASTUPDATE: 0x8, // hex for binary 1000
  AVG: 0x10, // hex for binary 10000
  LASTVOLUME: 0x20, // hex for binary 100000
  LASTVOLUMETO: 0x40, // hex for binary 1000000
  LASTTRADEID: 0x80, // hex for binary 10000000
  VOLUMEHOUR: 0x100, // hex for binary 100000000
  VOLUMEHOURTO: 0x200, // hex for binary 1000000000
  VOLUME24HOUR: 0x400, // hex for binary 10000000000
  VOLUME24HOURTO: 0x800, // hex for binary 100000000000
  OPENHOUR: 0x1000, // hex for binary 1000000000000
  HIGHHOUR: 0x2000, // hex for binary 10000000000000
  LOWHOUR: 0x4000, // hex for binary 100000000000000
  OPEN24HOUR: 0x8000, // hex for binary 1000000000000000
  HIGH24HOUR: 0x10000, // hex for binary 10000000000000000
  LOW24HOUR: 0x20000, // hex for binary 100000000000000000
  LASTMARKET: 0x40000 // hex for binary 1000000000000000000, this is a special case and will only appear on CCCAGG messages
};

CURRENT.DISPLAY = CURRENT.DISPLAY || {};
CURRENT.DISPLAY.FIELDS = {
  TYPE: { Show: false },
  MARKET: { Show: true, Filter: "Market" },
  FROMSYMBOL: { Show: false },
  TOSYMBOL: { Show: false },
  FLAGS: { Show: false },
  PRICE: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  BID: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  OFFER: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  LASTUPDATE: { Show: true, Filter: "Date", Format: "yyyy MMMM dd HH:mm:ss" },
  AVG: { Show: true, " Filter": "Number", Symbol: "TOSYMBOL" },
  LASTVOLUME: { Show: true, Filter: "Number", Symbol: "FROMSYMBOL" },
  LASTVOLUMETO: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  LASTTRADEID: { Show: true, Filter: "String" },
  VOLUMEHOUR: { Show: true, Filter: "Number", Symbol: "FROMSYMBOL" },
  VOLUMEHOURTO: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  VOLUME24HOUR: { Show: true, Filter: "Number", Symbol: "FROMSYMBOL" },
  VOLUME24HOURTO: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  OPENHOUR: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  HIGHHOUR: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  LOWHOUR: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  OPEN24HOUR: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  HIGH24HOUR: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  LOW24HOUR: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" },
  LASTMARKET: { Show: true, Filter: "String" },
  CHANGE24HOUR: { Show: true, Filter: "String" },
  CHANGE24HOURPCT: { Show: true, Filter: "String" },
  FULLVOLUMEFROM: { Show: true, Filter: "Number", Symbol: "FROMSYMBOL" },
  FULLVOLUMETO: { Show: true, Filter: "Number", Symbol: "TOSYMBOL" }
};

CURRENT.pack = function(currentObject) {
  var mask = 0;
  var packedCurrent = "";
  for (var property in this.FIELDS) {
    if (currentObject.hasOwnProperty(property)) {
      packedCurrent += "~" + currentObject[property];
      mask |= this.FIELDS[property];
    }
  }
  //removing first character beacsue it is a ~
  return packedCurrent.substr(1) + "~" + mask.toString(16);
};

CURRENT.unpack = function(value) {
  var valuesArray = value.split("~");
  var valuesArrayLenght = valuesArray.length;
  var mask = valuesArray[valuesArrayLenght - 1];
  var maskInt = parseInt(mask, 16);
  var unpackedCurrent = {};
  var currentField = 0;
  for (var property in this.FIELDS) {
    if (this.FIELDS[property] === 0) {
      unpackedCurrent[property] = valuesArray[currentField];
      currentField++;
    } else if (maskInt & this.FIELDS[property]) {
      //i know this is a hack, for cccagg, future code please don't hate me:(, i did this to avoid
      //subscribing to trades as well in order to show the last market
      if (property === "LASTMARKET") {
        unpackedCurrent[property] = valuesArray[currentField];
      } else {
        unpackedCurrent[property] = parseFloat(valuesArray[currentField]);
      }
      currentField++;
    }
  }

  return unpackedCurrent;
};
CURRENT.getKey = function(currentObject) {
  return (
    currentObject["TYPE"] +
    "~" +
    currentObject["MARKET"] +
    "~" +
    currentObject["FROMSYMBOL"] +
    "~" +
    currentObject["TOSYMBOL"]
  );
};
CURRENT.getKeyFromStreamerData = function(streamerData) {
  var valuesArray = streamerData.split("~");
  return (
    valuesArray[0] +
    "~" +
    valuesArray[1] +
    "~" +
    valuesArray[2] +
    "~" +
    valuesArray[3]
  );
};
