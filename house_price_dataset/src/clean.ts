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
dayjs.extend(customParseFormat);

/* ---------- CLI ---------- */
const SRC =
  process.argv[2] || path.join(__dirname, "../dataset/taipei_rent.csv");
const DEST =
  process.argv[3] || path.join(__dirname, "../dataset/taipei_rent_clean.csv");

/* ---------- Const ---------- */
const DROP_COLS = new Set([
  "都市土地使用分區",
  "非都市土地使用分區",
  "非都市土地使用編定",
  "備註",
]);
const PURPOSE_RE =
  /住家用|住宅|集合住宅|多戶住宅|國民住宅|公寓|雙併住宅|農舍|住商用|住工用|宿舍|寄宿|住宿單元/;
const EQUIP_SEP = /[、,，]/;
const MAX_LAYOUT = 100;
const DAY_MS = 86_400_000;

/* --- Header Fixed Order --- */
const HEADER_ORDER = [
  "編號",
  "交易標的",
  "鄉鎮市區",
  "土地位置建物門牌",
  "租賃年月日",
  "總額元",
  "出租型態",
  "租賃天數",
  "主要用途",
  "租賃層次",
  "總樓層數",
  "建物型態",
  "交易筆棟數-土地",
  "交易筆棟數-建物",
  "交易筆棟數-車位",
  "租賃住宅服務",
  "有無管理組織",
  "有無管理員",
  "有無附傢俱",
  "有無電梯",
  "主要建材",
  "建物現況格局-房",
  "建物現況格局-廳",
  "建物現況格局-衛",
  "建物現況格局-隔間",
  "土地面積平方公尺",
  "建物總面積平方公尺",
  "單價元平方公尺",
  "車位類別",
  "車位面積平方公尺",
  "車位總額元",
  "建築完成年月",
  "附屬設備-冷氣",
  "附屬設備-熱水器",
  "附屬設備-洗衣機",
  "附屬設備-電視機",
  "附屬設備-冰箱",
  "附屬設備-瓦斯或天然氣",
  "附屬設備-有線電視",
  "附屬設備-網路",
  "source_file",
];

/* ---------- Helpers ---------- */
/** 民國日期字串 → dayjs；支援 6/7 位純數字、`.0`、各種分隔符 */
function parseROC(raw: string): dayjs.Dayjs | null {
  let s = (raw ?? "").toString().trim();
  if (!s || s.toLowerCase() === "nan") return null;
  if (s.endsWith(".0")) s = s.slice(0, -2);

  // 6 or 7 digits
  if (/^\d{6,7}$/.test(s)) {
    if (s.length === 6) s = `0${s}`; // 6位補一位
    const yyy = +s.slice(0, 3);
    const mm = +s.slice(3, 5);
    const dd = +s.slice(5, 7);
    const d = dayjs(`${yyy + 1911}-${mm}-${dd}`, "YYYY-M-D", true);
    return d.isValid() ? d : null;
  }

  // 其他含分隔符
  const m = s.match(/(\d{1,3})[./年\-](\d{1,2})[./月\-](\d{1,2})/);
  if (!m) return null;
  const d = dayjs(`${+m[1] + 1911}-${m[2]}-${m[3]}`, "YYYY-M-D", true);
  return d.isValid() ? d : null;
}

const rocToISO = (s: string): string | null =>
  parseROC(s)?.format("YYYY-MM-DD") ?? null;

const periodToDays = (s: string): number | null => {
  const [a, b] = s.split("~").map((x) => parseROC(x)?.valueOf() ?? null);
  return a !== null && b !== null ? Math.round((b - a) / DAY_MS) : null;
};

/** 接受字串或數字並轉為數字；失敗回 null */
const toNum = (v: unknown): number | null => {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string") {
    const n = parseFloat(v.replace(/,/g, ""));
    return Number.isFinite(n) ? n : null;
  }
  return null;
};

/** 解析「土地/建物/車位」比棟數字串 */
function parseTransRatio(raw: string | undefined) {
  let land = 0,
    building = 0,
    parking = 0;
  if (!raw) return { land, building, parking };

  // 帶詞的情況
  const pairRE =
    /土地\s*[:：]?\s*(\d+(?:\.\d+)?)|建物\s*[:：]?\s*(\d+(?:\.\d+)?)|車位\s*[:：]?\s*(\d+(?:\.\d+)?)/g;
  let m: RegExpExecArray | null;
  while ((m = pairRE.exec(raw)) !== null) {
    if (m[1] !== undefined) land = parseFloat(m[1]);
    if (m[2] !== undefined) building = parseFloat(m[2]);
    if (m[3] !== undefined) parking = parseFloat(m[3]);
  }

  // 單純數字備援 (ex. "1 1 0")
  if (land === 0 && building === 0 && parking === 0) {
    const nums = raw.match(/\d+(?:\.\d+)?/g);
    if (nums && nums.length >= 3) {
      land = parseFloat(nums[0]);
      building = parseFloat(nums[1]);
      parking = parseFloat(nums[2]);
    }
  }
  return { land, building, parking };
}

function floorToNumber(chinese: string): number {
  const digitMap: Record<string, number> = {
    零: 0,
    一: 1,
    二: 2,
    兩: 2,
    三: 3,
    四: 4,
    五: 5,
    六: 6,
    七: 7,
    八: 8,
    九: 9,
  };

  // 1. Remove trailing "層"
  chinese = chinese.endsWith("層") ? chinese.slice(0, -1) : chinese;

  // 2. Check for "地下" (negative floor)
  const isNegative = chinese.startsWith("地下");
  if (isNegative) {
    chinese = chinese.slice(2);
  }

  // 3. Convert the remaining Chinese number (0–99)
  let result = 0;

  if (chinese.length === 1) {
    result = digitMap[chinese] ?? 0;
  } else {
    const tenIndex = chinese.indexOf("十");
    if (tenIndex !== -1) {
      const tens = tenIndex === 0 ? 1 : digitMap[chinese[tenIndex - 1]] ?? 1;
      result += tens * 10;

      const onesChar = chinese[tenIndex + 1];
      if (onesChar && digitMap[onesChar] !== undefined) {
        result += digitMap[onesChar];
      }
    } else {
      result = digitMap[chinese] ?? 0;
    }
  }

  return isNegative ? -result : result;
}

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
  | "租賃筆棟數不符"
  | "建築完成年月缺失"
  | "租賃年月日缺失"
  | "租賃層次不明"
  | "房過大"
  | "廳過大"
  | "衛過大"
  | "總額缺失";
const removed: Record<Reason, number> = {
  用途不符: 0,
  租賃筆棟數不符: 0,
  建築完成年月缺失: 0,
  租賃年月日缺失: 0,
  租賃層次不明: 0,
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

  /* --- transform --- */
  const out: Record<string, unknown> = {};
  out["編號"] = row["編號"];
  out["租賃年月日"] = leaseISO;
  out["建築完成年月"] = builtISO;

  out["租賃層次"] =
    row["租賃層次"] === "全" ? "NA" : floorToNumber(row["租賃層次"]);

  out["交易筆棟數-土地"] = land;
  out["交易筆棟數-建物"] = building;
  out["交易筆棟數-車位"] = parking;

  out["租賃天數"] = periodToDays(row["租賃期間"] ?? "") ?? "NA";

  out["主要用途"] = row["主要用途"];
  out["主要建材"] = row["主要建材"]?.trim() || "NA";

  out["建物現況格局-房"] = rooms;
  out["建物現況格局-廳"] = halls;
  out["建物現況格局-衛"] = baths;

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

  out["總額元"] = total;

  /* --- copy other cols, omit 附屬設備 / 租賃期間 / drop cols --- */
  Object.entries(row).forEach(([col, v]) => {
    if (
      [
        "附屬設備",
        "租賃期間",
        "租賃筆棟數",
        "建築完成年月",
        "編號",
        "租賃年月日",
        "備註",
      ].includes(col) ||
      DROP_COLS.has(col)
    )
      return;
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

  out["source_file"] = row["source_file"] ?? "";
  cleaned.push(out);
});

/* ---------- header order ---------- */
const extra = Object.keys(cleaned[0]).filter((k) => !HEADER_ORDER.includes(k));
const orderedCols = [...HEADER_ORDER, ...extra];

/* ---------- write ---------- */
fs.writeFileSync(DEST, Papa.unparse(cleaned, { columns: orderedCols }), "utf8");

/* ---------- stats ---------- */
const totalRemoved = Object.values(removed).reduce((a, b) => a + b, 0);
console.log("🧹  Cleaning summary");
Object.entries(removed).forEach(([k, v]) => console.log(`  • ${k}: ${v} rows`));
console.log(`Total removed: ${totalRemoved}`);
console.log(`Remaining rows: ${cleaned.length}`);
console.log(`✅  Clean file saved to ${DEST}`);
