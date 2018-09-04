// @flow

import type { Database } from "../types";
import { promisify } from "../utils";

const MongoClient = require("mongodb").MongoClient;

const url =
  process.env.MONGODB_URI || "mongodb://localhost:27017/ledger-countervalue";

const connect = () =>
  promisify(MongoClient, "connect", url, { useNewUrlParser: true });

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
  await promisify(
    db.collection("marketcap_coins"),
    "createIndex",
    {
      day: 1
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
  try {
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
  } finally {
    client.close();
  }
}

async function updateHistodays(id, histodays) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  try {
    await promisify(coll, "updateOne", { id }, { $set: { histodays } });
  } finally {
    client.close();
  }
}

async function updateExchanges(exchanges) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("exchanges");
  try {
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
  } finally {
    client.close();
  }
}

async function insertPairExchangeData(pairExchanges) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  try {
    await Promise.all(
      pairExchanges.map(pairExchange =>
        // we don't insert if it already exist to not override existing data.
        promisify(coll, "insertOne", pairExchange).catch(() => null)
      )
    );
  } finally {
    client.close();
  }
}

async function updatePairExchangeStats(id, stats) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("pairExchanges");
  try {
    await promisify(coll, "updateOne", { id }, { $set: stats });
  } finally {
    client.close();
  }
}

async function updateMarketCapCoins(day, coins) {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("marketcap_coins");
  try {
    await promisify(
      coll,
      "updateOne",
      { day },
      { day, coins },
      {
        upsert: true
      }
    );
  } finally {
    client.close();
  }
}

async function queryExchanges() {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("exchanges");
  try {
    const docs = await promisify(coll.find(), "toArray");
  } finally {
    client.close();
  }
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
  try {
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
  } finally {
    client.close();
  }
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
  try {
    const docs = await promisify(
      queryPairExchangesSortCursor(coll.find(query)),
      "toArray"
    );
    return docs;
  } finally {
    client.close();
  }
}

const queryPairExchangeById = async id => {
  const client = await connect();
  const db = client.db();
  const histodaysCol = db.collection("pairExchanges");
  try {
    const doc = await promisify(histodaysCol, "findOne", { id });
    return doc;
  } finally {
    client.close();
  }
};

const queryMarketCapCoinsForDay = async day => {
  const client = await connect();
  const db = client.db();
  const coll = db.collection("marketcap_coins");
  try {
    const doc = await promisify(coll, "findOne", { day });
    return doc && doc.coins;
  } finally {
    client.close();
  }
};

const database: Database = {
  init,
  statusDB,
  updateLiveRates,
  updateHistodays,
  updateExchanges,
  insertPairExchangeData,
  updatePairExchangeStats,
  updateMarketCapCoins,
  queryExchanges,
  queryPairExchangesByPairs,
  queryPairExchangesByPair,
  queryPairExchangeById,
  queryMarketCapCoinsForDay
};

export default database;
