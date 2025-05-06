import { crawlCloudTCM } from "./crawlers/cloudtcm";
import { crawlKMUH } from "./crawlers/kmuh";
import { crawlPingMing } from "./crawlers/pingming";
import { crawlHealing } from "./crawlers/healing";
import { mapEn2Zh, key } from "./utils/normalize";
import { saveCSV } from "./utils/csv";
import { RawRow, MergedRow, Nature } from "./types";

(async () => {
  const raw: RawRow[] = [
    // ...(await crawlCloudTCM()),
    ...(await crawlKMUH()),
    // ...(await crawlPingMing()),
    // ...(await crawlHealing()),
  ];

  // 合併
  const merged = new Map<string, MergedRow>();

  for (const row of raw) {
    const k = key(row.zh);
    const cur = merged.get(k);
    if (!cur) {
      merged.set(k, {
        zh: row.zh,
        nature: row.nature,
        variants: [],
        sources: [row.source],
      });
    } else {
      if (!cur.sources.includes(row.source)) cur.sources.push(row.source);
      if (cur.nature !== row.nature && !cur.variants!.includes(row.nature)) {
        cur.variants!.push(row.nature);
      }
    }
  }

  saveCSV([...merged.values()]);
  console.log(`DONE: ${merged.size} rows written to foods.csv`);
})();
