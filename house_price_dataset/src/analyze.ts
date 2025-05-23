#!/usr/bin/env ts-node
/**
 * analyze.ts – profile of taipei_rent_clean.csv  (v2025-05-17 R2)
 *
 * • Skip   : 編號、土地位置建物門牌、備註
 * • `租賃年月日`、`建築完成年月` → 只統計到「YYYY-MM」
 *
 * 前置摘要：來源檔路徑、筆數、欄位數、分析日期、排序依據
 *
 * Usage
 * ------
 *   ts-node analyze.ts [csvPath] [outputPath] [order]
 *
 *   • csvPath   (opt)  default = ./dataset/taipei_rent_clean.csv
 *   • outputPath(opt)  default = ./analysis/analyze_clean.txt
 *   • order     (opt)  "item" | "amount"   default = "amount"
 *
 * Example
 *   ts-node analyze.ts                        # 按數量排序 (預設)
 *   ts-node analyze.ts item                   # 按值字典序
 *   ts-node analyze.ts ./my.csv ./out.txt item
 */

import fs from "fs";
import path from "path";
import Papa from "papaparse";

/* ── CLI ─────────────────────────── */
const args = process.argv.slice(2).filter(Boolean);

const CSV_PATH =
  args.find((a) => a.endsWith(".csv")) ||
  path.join(__dirname, "../dataset/rent_clean.csv");

const OUT_PATH =
  args.find((a) => a.endsWith(".txt")) ||
  path.join(__dirname, "../analysis/anal_cln.txt");

const ORDER: "item" | "amount" = args.includes("item") ? "item" : "amount";

/* ── 常數 ─────────────────────────── */
const SKIP = new Set(["編號", "土地位置建物門牌", "備註"]);
const MONTH_COLS = new Set(["租賃年月日", "建築完成年月"]);

/* ── 讀檔 ─────────────────────────── */
if (!fs.existsSync(CSV_PATH)) {
  console.error(`❌  File not found: ${CSV_PATH}`);
  process.exit(1);
}
fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });

console.log("🔍  Analyzing …");

const { data, errors, meta } = Papa.parse<Record<string, string>>(
  fs.readFileSync(CSV_PATH, "utf8"),
  { header: true, skipEmptyLines: true, dynamicTyping: false }
);
if (errors.length) {
  console.error("❌  CSV parse errors:\n", errors);
  process.exit(1);
}

const rowCount = data.length;
const colCount = meta.fields?.length ?? Object.keys(data[0] || {}).length;
const analysisDate = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

/* ── 統計 ─────────────────────────── */
const freq: Record<string, Record<string, number>> = {};

data.forEach((row) => {
  Object.entries(row).forEach(([col, raw]) => {
    if (SKIP.has(col)) return;

    let val = (raw ?? "").trim();
    if (MONTH_COLS.has(col) && val) {
      const m = val.match(/^(\d{4}-\d{2})/);
      val = m ? m[1] : val;
    }

    const map = (freq[col] ??= {});
    map[val] = (map[val] || 0) + 1;
  });
});

/* ── 產生輸出 ─────────────────────── */
const lines: string[] = [];

/* 前置摘要 */
lines.push("台北市租屋資料集分析報告");
lines.push("==================================");
lines.push("資料來源  : https://plvr.land.moi.gov.tw/DownloadSeason/");
lines.push(`來源資料集: ${path.resolve(CSV_PATH)}`);
lines.push(`資料筆數  : ${rowCount}`);
lines.push(`欄位數    : ${colCount}`);
lines.push(`分析日期  : ${analysisDate}`);
lines.push(`排序方式  : Order by ${ORDER}`);
lines.push(""); // 空白行

/* 各欄統計 */
Object.entries(freq).forEach(([col, map]) => {
  lines.push(`${col}: ${Object.keys(map).length}`);
  const list = Object.entries(map);

  if (ORDER === "item") {
    list.sort((a, b) => a[0].localeCompare(b[0], "zh-Hant"));
  } else {
    list.sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], "zh-Hant"));
  }

  list.forEach(([v, c]) => lines.push(`\t${v || "<empty>"}: ${c}`));
  lines.push("");
});

/* ── 寫檔 & 完成 ──────────────────── */
fs.writeFileSync(OUT_PATH, lines.join("\n"), "utf8");
console.log(`✅  Analysis saved to ${path.resolve(OUT_PATH)}`);
