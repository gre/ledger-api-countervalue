// @flow

import type { Pair, Histodays, DB_Symbol, DB_Exchange } from "./types";
import { promisify } from "./utils";

const MongoClient = require("mongodb").MongoClient;

const url =
  process.env.MONGODB_URI || "mongodb://localhost:27017/ledger-countervalue";

const connect = () => promisify(MongoClient, "connect", url);

export const init = async () => {
  const client = await connect();
  const db = client.db();
  await promisify(
    db.collection("exchanges"),
    "createIndex",
    {
      id: 1
    },
    { unique: true }
  );
  await promisify(db.collection("symbols"), "createIndex", {
    from_to: 1
  });
  await promisify(
    db.collection("symbols"),
    "createIndex",
    {
      id: 1
    },
    { unique: true }
  );
  client.close();
};

export async function statusDB() {
  const client = await connect();
  const db = client.db();
  try {
    const coll = db.collection("symbols");
    const count = await promisify(coll, "count");
    if (count === 0) throw new Error("database is empty");
  } finally {
    client.close();
  }
}

export async function updateLiveRates(
  all: Array<{
    symbol: string,
    rate: number
  }>
) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("symbols");
  await Promise.all(
    all.map(item =>
      promisify(
        coll,
        "update",
        { id: item.symbol },
        {
          $set: {
            latest: item.rate,
            latestDate: new Date()
          }
        }
      )
    )
  );
  client.close();
}

export async function updateHistodays(symbol: string, histodays: Histodays) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("symbols");
  await promisify(coll, "update", { id: symbol }, { $set: { histodays } });
  client.close();
}

export async function updateExchanges(exchanges: DB_Exchange[]) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("exchanges");
  await Promise.all(
    exchanges.map(exchange =>
      promisify(coll, "update", { id: exchange.id }, exchange, {
        upsert: true
      })
    )
  );
  client.close();
}

export async function updateAssetSymbols(symbols: DB_Symbol[]) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("symbols");
  await Promise.all(
    symbols.map(symbol =>
      promisify(coll, "update", { id: symbol.id }, symbol, { upsert: true })
    )
  );
  client.close();
}

export async function updateSymbolStats(
  symbol: string,
  stats: {
    yesterdayVolume?: number,
    hasHistoryFor30LastDays?: boolean,
    historyLoadedAtDay?: string,
    latestDate?: Date
  }
) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("symbols");
  await promisify(coll, "update", { id: symbol }, { $set: stats });
  client.close();
}

export async function queryExchanges(): Promise<DB_Exchange[]> {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("exchanges");
  const docs = await promisify(coll.find(), "toArray");
  client.close();
  return docs;
}

const querySymbolsSortCursor = cursor =>
  cursor.sort({
    yesterdayVolume: -1
  });

export async function querySymbolsByPairs(pairs: Pair[]): Promise<DB_Symbol[]> {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("symbols");
  const docs = await promisify(
    querySymbolsSortCursor(
      coll.find({
        from_to: {
          $in: pairs.map(p => p.from + "_" + p.to)
        }
      })
    ),
    "toArray"
  );
  client.close();
  return docs;
}

export async function querySymbolsByPair(
  pair: Pair,
  opts?: { filterWithHistory?: boolean } = {}
): Promise<DB_Symbol> {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("symbols");
  const { from, to } = pair;
  const query: Object = { from, to };
  if (opts.filterWithHistory) {
    query.hasHistoryFor30LastDays = true;
  }
  const docs = await promisify(
    querySymbolsSortCursor(coll.find(query)),
    "toArray"
  );
  client.close();
  return docs;
}

export const querySymbolById = async (symbol: string): Promise<DB_Symbol> => {
  const client = await connect();
  const db = client.db();
  const histodaysCol = db.collection("symbols");
  const doc = await promisify(histodaysCol, "findOne", { id: symbol });
  client.close();
  return doc;
};
