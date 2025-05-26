/**
 * ========================================================================== *
 *  File        : clean.ts                                                    *
 *  Purpose     : Deep-clean raw rent data and engineer core features.        *
 *                                                                            *
 *  Usage       : ts-node clean.ts [source_file] [export_file]                *
 *                                                                            *
 *  Workflow    : 1) Load rent_ori.csv                                        *
 *                2) Drop rows via getRemovalReason()                         *
 *                3) transformRow(): dates → ISO, 屋齡, flags, enums …        *
 *                4) Drop DROP_COLS columns                                   *
 *                5) Save rent_cln.csv & log summary                          *
 *                                                                            *
 *  Source file : /root/dataset/rent_ori.csv                                  *
 *  Export file : /root/dataset/rent_cln.csv                                  *
 *  Simple rules: 住宅用途, 單價>0, 建材/層次≠「見其他」, layout≤100 …           *
 *  Updated     : 2025-05-26                                                  *
 * ========================================================================== *
 */

import fs from "node:fs";
import path from "node:path";
import Papa from "papaparse";
import dayjs from "dayjs";
import customParseFormat from "dayjs/plugin/customParseFormat";
import {
  getRemovalReason,
  parseTransRatio,
  transformRow,
} from "./components/helpers";
import { CleanRow, RawRow, RemoveRow } from "./types";
dayjs.extend(customParseFormat);

/* ---------- CLI ---------- */
const SRC = process.argv[2] || path.join(__dirname, "../dataset/rent_ori.csv");
const DEST = process.argv[3] || path.join(__dirname, "../dataset/rent_cln.csv");

/* ---------- Const ---------- */
export const REASONS = [
  "用途不符",
  "交易標的不符",
  "租賃筆棟數不符",
  "建築完成年月缺失",
  "租賃年月日缺失",
  "租賃層次不明",
  "主要建材不明",
  "單價為零",
  "無須車位出租相關資訊",
  "房過大",
  "廳過大",
  "衛過大",
  "總額缺失",
] as const;

export const DROP_COLS = new Set([
  "交易標的",
  "土地面積平方公尺",
  "都市土地使用分區",
  "非都市土地使用分區",
  "非都市土地使用編定",
  "租賃筆棟數",
  "租賃期間",
  "車位類別",
  "車位面積平方公尺",
  "車位總額元",
  "附屬設備",
  "備註",
  "source_file",
]);

export const PURPOSE_RE =
  /住家用|住宅|集合住宅|多戶住宅|國民住宅|公寓|雙併住宅|農舍|住商用|住工用|宿舍|寄宿|住宿單元/;
export const EQUIP_SEP = /[、,，]/;
export const MAX_LAYOUT = 100;

const removed: RemoveRow = Object.fromEntries(
  REASONS.map((r) => [r, 0])
) as RemoveRow;
const cleaned: CleanRow[] = [];

/* ---------- Read CSV ---------- */
if (!fs.existsSync(SRC)) {
  console.error(`❌  File not found: ${SRC}`);
  process.exit(1);
}
console.log("🔍  Cleaning …");

const { data } = Papa.parse<RawRow>(fs.readFileSync(SRC, "utf8"), {
  header: true,
  skipEmptyLines: true,
  dynamicTyping: false,
});

/* ---------- Collect equipment set ---------- */
export const EQUIP_COLS = [
  ...new Set(
    data.flatMap((r) =>
      (r["附屬設備"] ?? "")
        .split(EQUIP_SEP)
        .map((x) => x.trim())
        .filter(Boolean)
    )
  ),
];

/* ---------- Main loop ---------- */
for (const row of data) {
  const parsedTransRatio = parseTransRatio(row["租賃筆棟數"]);
  const reason = getRemovalReason(row, parsedTransRatio);
  if (reason) {
    removed[reason]++;
    continue;
  }
  cleaned.push(transformRow(row, parsedTransRatio));
}

/* ---------- write ---------- */
fs.writeFileSync(DEST, Papa.unparse(cleaned), "utf8");

/* ---------- stats ---------- */
const totalRemoved = Object.values(removed).reduce((a, b) => a + b, 0);
console.log("🧹  Cleaning summary");
Object.entries(removed).forEach(([k, v]) => console.log(`  • ${k}: ${v} rows`));
console.log(`Total removed: ${totalRemoved}`);
console.log(`Remaining rows: ${cleaned.length}`);
console.log(`✅  Clean file saved to ${DEST}`);
