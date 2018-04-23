// @flow
// Synchronize the local database with distant service
import { init } from "./db";
import { prefetchAllSymbols, pullLiveRates } from "./cache";
import { pullLiveRatesDebugMessage, pullLiveRatesError } from "./logger";
import { recurrentJob } from "./utils";

init().then(() => {
  pullLiveRates(
    msg => {
      if (process.env.DEBUG_LIVE_RATES) pullLiveRatesDebugMessage(msg);
    },
    error => {
      pullLiveRatesError(error);
      process.exit(1);
    }
  );
  if (!process.env.DISABLE_PREFETCH) {
    recurrentJob(prefetchAllSymbols, 4 * 60 * 60 * 1000);
  }
});
