// @flow
import { logError } from "./logger";

process.on("uncaughtException", err => {
  logError("uncaughtException", err);
  process.exit(1);
});
