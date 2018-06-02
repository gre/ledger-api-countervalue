// @flow

import type { Database } from "../types";
import { promisify } from "../utils";

const MongoClient = require("mongodb").MongoClient;

const url =
  process.env.MONGODB_URI || "mongodb://localhost:27017/ledger-countervalue";

const connect = () => promisify(MongoClient, "connect", url);

const init = async () => {
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
  await promisify(db.collection("pairExchanges"), "createIndex", {
    from_to: 1
  });
  await promisify(
    db.collection("pairExchanges"),
    "createIndex",
    {
      id: 1
    },
    { unique: true }
  );
  client.close();
};

async function statusDB() {
  const client = await connect();
  const db = client.db();
  try {
    const coll = db.collection("pairExchanges");
    const count = await promisify(coll, "count");
    if (count === 0) throw new Error("database is empty");
  } finally {
    client.close();
  }
}

async function updateLiveRates(all) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  await Promise.all(
    all.map(item =>
      promisify(
        coll,
        "update",
        { id: item.pairExchangeId },
        {
          $set: {
            latest: item.price,
            latestDate: new Date()
          }
        }
      )
    )
  );
  client.close();
}

async function updateHistodays(id, histodays) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  await promisify(coll, "update", { id }, { $set: { histodays } });
  client.close();
}

async function updateExchanges(exchanges) {
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

async function insertPairExchangeData(pairExchanges) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  await Promise.all(
    pairExchanges.map(pairExchange =>
      // we don't insert if it already exist to not override existing data.
      promisify(coll, "insert", pairExchange).catch(() => null)
    )
  );
  client.close();
}

async function updatePairExchangeStats(id, stats) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  await promisify(coll, "update", { id }, { $set: stats });
  client.close();
}

async function queryExchanges() {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("exchanges");
  const docs = await promisify(coll.find(), "toArray");
  client.close();
  return docs;
}

const queryPairExchangesSortCursor = cursor =>
  cursor.sort({
    yesterdayVolume: -1
  });

async function queryPairExchangesByPairs(pairs) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  const docs = await promisify(
    queryPairExchangesSortCursor(
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

async function queryPairExchangesByPair(pair, opts = {}) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  const { from, to } = pair;
  const query: Object = { from, to };
  if (opts.filterWithHistory) {
    query.hasHistoryFor30LastDays = true;
  }
  const docs = await promisify(
    queryPairExchangesSortCursor(coll.find(query)),
    "toArray"
  );
  client.close();
  return docs;
}

const queryPairExchangeById = async id => {
  const client = await connect();
  const db = client.db();
  const histodaysCol = db.collection("pairExchanges");
  const doc = await promisify(histodaysCol, "findOne", { id });
  client.close();
  return doc;
};

const database: Database = {
  init,
  statusDB,
  updateLiveRates,
  updateHistodays,
  updateExchanges,
  insertPairExchangeData,
  updatePairExchangeStats,
  queryExchanges,
  queryPairExchangesByPairs,
  queryPairExchangesByPair,
  queryPairExchangeById
};

export default database;
