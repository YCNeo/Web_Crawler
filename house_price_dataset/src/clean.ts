#!/usr/bin/env ts-node
/**
 * clean.ts – Taipei rent dataset cleaner (2025-05-16)
 *
 * • Input : ./dataset/taipei_rent.csv   (或 CLI argv[2])
 * • Output: ./dataset/taipei_rent_clean.csv (或 CLI argv[3])
 * • Terminal 只列刪除統計與剩餘筆數
 */

import fs from "node:fs";
import path from "node:path";
import Papa from "papaparse";
import dayjs from "dayjs";
import customParseFormat from "dayjs/plugin/customParseFormat";
import {
  buildingMaterialClassify,
  floorChange,
  houseAgeClassify,
  parseTransRatio,
  periodToDays,
  purposeClassify,
  rocToISO,
  toNum,
} from "./components/helpers";
dayjs.extend(customParseFormat);

/* ---------- CLI ---------- */
const SRC = process.argv[2] || path.join(__dirname, "../dataset/rent_ori.csv");
const DEST = process.argv[3] || path.join(__dirname, "../dataset/rent_cln.csv");

/* ---------- Const ---------- */
const DROP_COLS = new Set([
  "交易標的",
  "土地面積平方公尺",
  "都市土地使用分區",
  "非都市土地使用分區",
  "非都市土地使用編定",
  "租賃筆棟數",
  "租賃期間",
  "租賃年月日",
  "建築完成年月",
  "車位類別",
  "車位面積平方公尺",
  "車位總額元",
  "附屬設備",
  "備註",
  "source_file",
]);
const PURPOSE_RE =
  /住家用|住宅|集合住宅|多戶住宅|國民住宅|公寓|雙併住宅|農舍|住商用|住工用|宿舍|寄宿|住宿單元/;
const EQUIP_SEP = /[、,，]/;
const MAX_LAYOUT = 100;

/* ---------- Read CSV ---------- */
if (!fs.existsSync(SRC)) {
  console.error(`❌  File not found: ${SRC}`);
  process.exit(1);
}
console.log("🔍  Cleaning …");

const { data } = Papa.parse<Record<string, string>>(
  fs.readFileSync(SRC, "utf8"),
  {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: false,
  }
);

/* ---------- Collect equipment set ---------- */
const equipSet = new Set<string>();
data.forEach((r) =>
  (r["附屬設備"] ?? "")
    .split(EQUIP_SEP)
    .map((x) => x.trim())
    .filter(Boolean)
    .forEach((e) => equipSet.add(e))
);
const EQUIP_COLS = Array.from(equipSet);

/* ---------- Cleaning ---------- */
type Reason =
  | "用途不符"
  | "交易標的不符"
  | "租賃筆棟數不符"
  | "建築完成年月缺失"
  | "租賃年月日缺失"
  | "租賃層次不明"
  | "主要建材不明"
  | "單價為零"
  | "無須車位出租相關資訊"
  | "房過大"
  | "廳過大"
  | "衛過大"
  | "總額缺失";
const removed: Record<Reason, number> = {
  用途不符: 0,
  交易標的不符: 0,
  租賃筆棟數不符: 0,
  建築完成年月缺失: 0,
  租賃年月日缺失: 0,
  租賃層次不明: 0,
  主要建材不明: 0,
  單價為零: 0,
  無須車位出租相關資訊: 0,
  房過大: 0,
  廳過大: 0,
  衛過大: 0,
  總額缺失: 0,
};

const cleaned: Record<string, unknown>[] = [];

data.forEach((row) => {
  /* --- delete rules --- */
  if (!PURPOSE_RE.test(row["主要用途"] ?? "")) {
    removed["用途不符"]++;
    return;
  }

  if (row["交易標的"].includes("房地")) {
    removed["交易標的不符"]++;
    return;
  }

  const { land, building, parking } = parseTransRatio(row["租賃筆棟數"]);
  if (land === 0 && building === 0) {
    removed["租賃筆棟數不符"]++;
    return;
  }

  const builtISO = rocToISO(row["建築完成年月"] ?? "");
  if (builtISO === null) {
    removed["建築完成年月缺失"]++;
    return;
  }

  const leaseISO = rocToISO(row["租賃年月日"] ?? "");
  if (!leaseISO) {
    removed["租賃年月日缺失"]++;
    return;
  }

  const rooms = +row["建物現況格局-房"]!;
  if (rooms > MAX_LAYOUT) {
    removed["房過大"]++;
    return;
  }

  const halls = +row["建物現況格局-廳"]!;
  if (halls > MAX_LAYOUT) {
    removed["廳過大"]++;
    return;
  }

  const baths = +row["建物現況格局-衛"]!;
  if (baths > MAX_LAYOUT) {
    removed["衛過大"]++;
    return;
  }

  const total = toNum(row["總額元"]);
  if (total === null) {
    removed["總額缺失"]++;
    return;
  }

  if (row["租賃層次"] === "見其他登記事項") {
    removed["租賃層次不明"]++;
    return;
  }

  if (
    row["主要建材"] === "見其他登記事項" ||
    row["主要建材"] === "見使用執照"
  ) {
    removed["主要建材不明"]++;
    return;
  }

  if (row["單價元平方公尺"] === "") {
    removed["單價為零"]++;
    return;
  }

  if (row["車位類別"] !== "") {
    removed["無須車位出租相關資訊"]++;
    return;
  }

  /* --- transform --- */
  const out: Record<string, unknown> = {};
  out["租賃年月日"] = leaseISO;
  out["建築完成年月"] = builtISO;

  out["屋齡"] = dayjs(leaseISO).diff(dayjs(builtISO), "year");
  out["屋齡分類"] = houseAgeClassify(out["屋齡"] as number);

  out["交易筆棟數-土地"] = land;
  out["交易筆棟數-建物"] = building;
  out["交易筆棟數-車位"] = parking;

  out["租賃天數"] = periodToDays(row["租賃期間"] ?? "") ?? "NA";

  out["主要用途"] = row["主要用途"];
  out["主要用途分類"] = purposeClassify(row["主要用途"] ?? "");

  out["主要建材"] = row["主要建材"]?.trim() || "NA";

  out["建物現況格局-房"] = rooms;
  out["建物現況格局-廳"] = halls;
  out["建物現況格局-衛"] = baths;

  out["租賃層次(四類)"] = floorChange(row["租賃層次"]?.trim() || "NA");

  out["建材分類"] = buildingMaterialClassify(row["主要建材"]?.trim() || "NA");

  [
    "建物現況格局-隔間",
    "有無管理組織",
    "有無附傢俱",
    "有無電梯",
    "有無管理員",
  ].forEach((k) => {
    const v = row[k]?.trim();
    out[k] = v === "有" ? 1 : v === "無" ? 0 : "NA";
  });

  ["出租型態", "租賃住宅服務"].forEach((k) => {
    const v = row[k]?.trim();
    out[k] = v === "" ? "NA" : v;
  });

  out["總額元"] = total;

  /* --- copy other cols, omit 附屬設備 / 租賃期間 / drop cols --- */
  Object.entries(row).forEach(([col, v]) => {
    if (DROP_COLS.has(col)) return;
    if (!out.hasOwnProperty(col)) out[col] = v;
  });

  /* --- split equipments into 1/0 --- */
  const own = new Set(
    (row["附屬設備"] ?? "")
      .split(EQUIP_SEP)
      .map((x) => x.trim())
      .filter(Boolean)
  );
  EQUIP_COLS.forEach((eq) => {
    out[`附屬設備-${eq}`] = own.has(eq) ? 1 : 0;
  });

  cleaned.push(out);
});

/* ---------- write ---------- */
fs.writeFileSync(DEST, Papa.unparse(cleaned), "utf8");

/* ---------- stats ---------- */
const totalRemoved = Object.values(removed).reduce((a, b) => a + b, 0);
console.log("🧹  Cleaning summary");
Object.entries(removed).forEach(([k, v]) => console.log(`  • ${k}: ${v} rows`));
console.log(`Total removed: ${totalRemoved}`);
console.log(`Remaining rows: ${cleaned.length}`);
console.log(`✅  Clean file saved to ${DEST}`);
