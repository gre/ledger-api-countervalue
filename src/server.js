// @flow
// Implement the HTTP server API

import "babel-polyfill";
import "./nodeCrashOnUncaught";
import express from "express";
import cors from "cors";
import bodyParser from "body-parser";
import { getExchanges, getHistoRequest, getDailyMarketCapCoins } from "./cache";
import type {
  HistoAPIRequest,
  RequestPair,
  Pair,
  HistoAPIResponse,
  ExchangesAPIRequest,
  ExchangesAPIResponse
} from "./types";
import { logEndpointError, logError, logEndpointCall } from "./logger";
import { getCurrentDatabase } from "./db";
import { supportTicker } from "./utils";
import { version } from "../package.json";

function endpoint<In, Out>(validateInput: mixed => In, f: In => Promise<Out>) {
  return (req: *, res: *) => {
    logEndpointCall(req);
    Promise.resolve(req)
      .then(validateInput)
      .then(request =>
        f(request).then(
          response => res.json(response),
          error => {
            logEndpointError(req, error);
            res.status(500).send();
          }
        )
      )
      .catch(error => {
        logEndpointError(req, error);
        res.status(400).send({ message: String(error.message) });
      });
  };
}

var app = express();

app.use(cors());

app.post(
  "/rates/:granularity",
  bodyParser.json(),
  endpoint(
    req => {
      const granularity =
        req && typeof req.params === "object" && req.params
          ? String(req.params.granularity)
          : "";
      if (granularity !== "daily" && granularity !== "hourly") {
        throw new Error("unsupported granularity=" + granularity);
      }
      if (typeof req !== "object" || !req || typeof req.body !== "object") {
        throw new Error("no body");
      }
      const json = req.body;
      // validate user input
      if (!json || typeof json !== "object") {
        throw new Error("invalid JSON input");
      }
      if (typeof json.pairs !== "object") {
        throw new Error("invalid JSON format: {pairs} missing");
      }
      const pairsUnsafe = json.pairs;
      if (
        !pairsUnsafe ||
        typeof pairsUnsafe !== "object" ||
        !Array.isArray(pairsUnsafe)
      ) {
        throw new Error("invalid JSON format: {pairs} missing");
      }
      const pairs = [];
      const dedupCheckMap = {};
      for (const o of pairsUnsafe) {
        if (!o || typeof o !== "object") continue;
        const { from, to, exchange, afterDay, at } = o;
        let { after } = o;
        if (granularity !== "daily" && "afterDay" in o) {
          throw new Error("afterDay is invalid. use 'after' instead");
        }
        if (!after) after = afterDay; // afterDay is deprecated but still accepted (safer until we migrate all clients)
        if (typeof from !== "string" || !from || typeof to !== "string" || !to)
          continue;
        if (!supportTicker(from) || !supportTicker(to)) continue;
        const pair: RequestPair = { from, to };
        if (exchange && typeof exchange !== "string") continue;
        if (after && typeof after !== "string") continue;
        let atValidated;
        if (at) {
          if (typeof at === "string") {
            atValidated = [at];
          } else if (Array.isArray(at)) {
            atValidated = at.filter(v => typeof v === "string");
          }
        }
        if (exchange) pair.exchange = exchange;
        if (after) pair.after = after;
        if (atValidated) pair.at = atValidated;
        const key = `${from}|${to}|${exchange || ""}`;
        if (key in dedupCheckMap) {
          throw new Error("invalid input: pairs must not contains duplicates");
        }
        dedupCheckMap[key] = 1;
        pairs.push(pair);
      }
      if (pairs.length === 0) {
        throw new Error("invalid input: pairs is empty");
      }
      if (pairs.length > 100) {
        throw new Error("invalid input: too much pairs requested");
      }
      return { pairs, granularity };
    },
    async (request: HistoAPIRequest) => {
      const r: HistoAPIResponse = await getHistoRequest(
        request.pairs,
        request.granularity
      );
      return r;
    }
  )
);

app.get("/_health", (req: *, res: *) => {
  const db = getCurrentDatabase();
  db.statusDB()
    .then(() => {
      res.status(200).send({
        status: "OK",
        service: "database",
        version
      });
    })
    .catch(error => {
      logEndpointError(req, error);
      res.status(503).send({
        status: "KO",
        service: "database"
      });
    });
});

app.get("/_health/noop", (req: *, res: *) => {
  res.status(200).send();
});

app.get("/_health/detail", (req: *, res: *) => {
  const db = getCurrentDatabase();
  Promise.all([db.statusDB(), db.getMeta()])
    .then(([, meta]) => {
      const liveSyncAgo = new Date() - meta.lastLiveRatesSync;
      const marketCapSyncAgo = new Date() - meta.lastMarketCapSync;

      const status = [
        {
          service: "database",
          status: "OK"
        },
        {
          service: "live-rates",
          status: liveSyncAgo > 5 * 60 * 1000 ? "KO" : "OK",
          lastDate: meta.lastLiveRatesSync
        },
        {
          service: "marketcap",
          status: marketCapSyncAgo > 25 * 60 * 60 * 1000 ? "KO" : "OK",
          lastDate: meta.lastMarketCapSync
        }
      ];
      res.status(status.some(s => s.status === "KO") ? 500 : 200).send(status);
    })
    .catch(error => {
      logEndpointError(req, error);
      res.status(503).send([
        {
          status: "KO",
          service: "database"
        }
      ]);
    });
});

app.get(
  "/exchanges/:from/:to",
  endpoint(
    req => {
      if (!req || !req.params || typeof req.params !== "object") {
        throw new Error("invalid request");
      }
      const { from, to } = req.params;
      if (typeof from !== "string" || !from || typeof to !== "string" || !to) {
        throw new Error("pair is missing {from, to}");
      }
      if (!supportTicker(from)) {
        throw new Error(`${from} is not supported`);
      }
      if (!supportTicker(to)) {
        throw new Error(`${to} is not supported`);
      }
      const pair: Pair = { from, to };
      return { pair };
    },
    async (request: ExchangesAPIRequest): Promise<ExchangesAPIResponse> => {
      const exchanges = await getExchanges(request);
      return exchanges;
    }
  )
);

app.get("/tickers", endpoint(() => null, getDailyMarketCapCoins));

if (process.env.HACK_SYNC_IN_SERVER) {
  require("./sync");
}

getCurrentDatabase()
  .init()
  .then(() => {
    console.log("DB initialized."); // eslint-disable-line no-console
    const port = process.env.PORT || 8088;
    app.listen(port, () => {
      console.log(`Server running on ${port}`); // eslint-disable-line no-console
    });
    getDailyMarketCapCoins().catch(e =>
      logError("marketcap failed to fetch", e)
    );
  });
