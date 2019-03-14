// @flow

import type { Database } from "../types";
import { promisify } from "../utils";

const MongoClient = require("mongodb").MongoClient;

const url =
  process.env.MONGODB_URI || "mongodb://localhost:27017/ledger-countervalue";

const connect = () =>
  promisify(MongoClient, "connect", url, { useNewUrlParser: true });

let dbPromise = null;
const getDB = () => {
  if (!dbPromise) {
    dbPromise = connect();
  }
  return dbPromise;
};

const init = async () => {
  const client = await getDB();
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
  await promisify(db.collection("marketcap_coins"), "remove");
  await promisify(
    db.collection("marketcap_coins"),
    "createIndex",
    {
      day: 1
    },
    { unique: true }
  );
};

const metaId = "meta_1";

async function setMeta(meta) {
  const client = await getDB();
  const db = client.db();
  await promisify(
    db.collection("meta"),
    "updateOne",
    { id: metaId },
    {
      $set: meta
    },
    { upsert: true }
  );
}

async function getMeta() {
  const client = await getDB();
  const db = client.db();
  const { id, _id, ...meta } = await promisify(
    db.collection("meta"),
    "findOne",
    {
      id: metaId
    }
  );
  return {
    lastLiveRatesSync: new Date(0),
    lastMarketCapSync: new Date(0),
    ...meta
  };
}

async function statusDB() {
  const client = await getDB();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  const count = await promisify(coll, "countDocuments");
  if (count === 0) throw new Error("database is empty");
}

async function updateLiveRates(all) {
  const client = await getDB();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  await Promise.all(
    all.map(item =>
      promisify(
        coll,
        "updateOne",
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
  await setMeta({ lastLiveRatesSync: new Date() });
}

async function updateHisto(id, granurity, histo) {
  const client = await getDB();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  await promisify(
    coll,
    "updateOne",
    { id },
    {
      $set: {
        [`histo_${granurity}`]: histo
      }
    }
  );
}

async function updateExchanges(exchanges) {
  const client = await getDB();
  const db = client.db();
  const coll = db.collection("exchanges");
  await Promise.all(
    exchanges.map(exchange =>
      promisify(
        coll,
        "updateOne",
        { id: exchange.id },
        { $set: exchange },
        {
          upsert: true
        }
      )
    )
  );
}

async function insertPairExchangeData(pairExchanges) {
  const client = await getDB();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  await Promise.all(
    pairExchanges.map(pairExchange =>
      // we don't insert if it already exist to not override existing data.
      promisify(coll, "insertOne", pairExchange).catch(() => null)
    )
  );
}

async function updatePairExchangeStats(id, stats) {
  const client = await getDB();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  await promisify(coll, "updateOne", { id }, { $set: stats });
}

async function updateMarketCapCoins(day, coins) {
  const client = await getDB();
  const db = client.db();
  const coll = db.collection("marketcap_coins");
  await promisify(
    coll,
    "updateOne",
    { day },
    { $set: { day, coins } },
    {
      upsert: true
    }
  );
  await setMeta({ lastMarketCapSync: new Date() });
}

async function queryExchanges() {
  const client = await getDB();
  const db = client.db();
  const coll = db.collection("exchanges");
  const docs = await promisify(coll.find(), "toArray");
  return docs;
}

const queryPairExchangesSortCursor = cursor =>
  cursor.sort({
    hasHistoryFor1Year: -1,
    yesterdayVolume: -1
  });

async function queryPairExchangesByPairs(pairs) {
  const client = await getDB();
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
  return docs;
}

async function queryPairExchangesByPair(pair, opts = {}) {
  const client = await getDB();
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
  return docs;
}

async function queryPairExchanges() {
  const client = await getDB();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  const docs = await promisify(
    queryPairExchangesSortCursor(coll.find()),
    "toArray"
  );
  return docs;
}

const queryPairExchangeById = async id => {
  const client = await getDB();
  const db = client.db();
  const histodaysCol = db.collection("pairExchanges");
  const doc = await promisify(histodaysCol, "findOne", { id });
  return doc;
};

const queryMarketCapCoinsForDay = async day => {
  const client = await getDB();
  const db = client.db();
  const coll = db.collection("marketcap_coins");
  const doc = await promisify(coll, "findOne", { day });
  return doc && doc.coins;
};

const database: Database = {
  init,
  getMeta,
  statusDB,
  updateLiveRates,
  updateHisto,
  updateExchanges,
  insertPairExchangeData,
  updatePairExchangeStats,
  updateMarketCapCoins,
  queryExchanges,
  queryPairExchanges,
  queryPairExchangesByPairs,
  queryPairExchangesByPair,
  queryPairExchangeById,
  queryMarketCapCoinsForDay
};

export default database;
