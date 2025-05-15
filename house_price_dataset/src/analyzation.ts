#!/usr/bin/env ts-node
/**
 * analyzation.ts
 * ---------------
 * Reads a CSV file (default: taipei_rent.csv) and **writes the full analysis** to
 *   ./analysis/analyze.txt  (or a custom path via CLI)
 *
 * The terminal will show **only minimal status messages**, _not_ the analysis
 * details, to keep stdout clean.
 *
 * Analysis rules:
 *   • Skip columns: 編號, 土地位置建物門牌, 備註
 *   • Binned histogram (10 bins) for numeric‑like columns:
 *       租賃期間 (→ 月數)、土地面積平方公尺、建物總面積平方公尺、單價元平方公尺、車位面積平方公尺
 *   • Year grouping for 租賃年月日
 *   • Categorical value counts for remaining columns
 *
 * Usage:
 *   ts-node analyzation.ts [csvPath] [outputPath]
 *   # outputPath optional, default = ./analysis/analyze.txt
 */

import fs from "node:fs";
import path from "node:path";
import Papa from "papaparse";

/* ---------- Configuration ---------- */
const skipCols = new Set(["編號", "土地位置建物門牌", "備註"]);
const numericBinCols = new Set([
  "租賃期間",
  "土地面積平方公尺",
  "建物總面積平方公尺",
  "單價元平方公尺",
  "車位面積平方公尺",
]);
const dateYearGroupCols = new Set(["租賃年月日"]);
const DEFAULT_BIN_COUNT = 10; // histogram bins

/* ---------- Helpers ---------- */
const toNumber = (str: string): number | null => {
  const cleaned = str.replace(/,/g, "");
  const num = parseFloat(cleaned);
  return isNaN(num) ? null : num;
};

const parseLeaseDuration = (str: string): number | null => {
  const m = str.match(/(?:(\d+)\s*年)?\s*(?:(\d+)\s*月)?/);
  if (!m) return null;
  const years = m[1] ? parseInt(m[1], 10) : 0;
  const months = m[2] ? parseInt(m[2], 10) : 0;
  const total = years * 12 + months;
  return total > 0 ? total : null;
};

const extractYear = (str: string): string | null => {
  const m = str.match(/^(\d{3,4})/);
  return m ? m[1] : null;
};

const formatNum = (n: number): string =>
  Number.isInteger(n) ? n.toString() : n.toFixed(2);

/* ---------- Main ---------- */
function main() {
  const csvPath =
    process.argv[2] || path.join(__dirname, "../dataset/taipei_rent.csv");
  const outputPath =
    process.argv[3] || path.join(process.cwd(), "analysis/analyze.txt");

  // Ensure parent directory exists
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });

  if (!fs.existsSync(csvPath)) {
    console.error(`❌  File not found: ${csvPath}`);
    process.exit(1);
  }

  console.log("🔍  Analyzing data …");

  const raw = fs.readFileSync(csvPath, "utf8");
  const parsed = Papa.parse<Record<string, string>>(raw, {
    header: true,
    skipEmptyLines: true,
  });

  if (parsed.errors.length) {
    console.error("❌  CSV parse errors detected:\n", parsed.errors);
    process.exit(1);
  }

  // Data containers
  const freqMap: Record<string, Record<string, number>> = {};
  const numericData: Record<string, number[]> = {};

  /* ----- Iterate rows ----- */
  parsed.data.forEach((row) => {
    Object.entries(row).forEach(([col, rawVal]) => {
      if (skipCols.has(col)) return;

      const value = (rawVal ?? "").trim();
      if (!value) return;

      // Numeric histogram columns
      if (numericBinCols.has(col)) {
        const num =
          col === "租賃期間" ? parseLeaseDuration(value) : toNumber(value);
        if (num !== null) (numericData[col] ??= []).push(num);
        return;
      }

      // Date columns grouped by year
      if (dateYearGroupCols.has(col)) {
        const yr = extractYear(value) || "(invalid)";
        const colFreq = (freqMap[col] ??= {});
        colFreq[yr] = (colFreq[yr] || 0) + 1;
        return;
      }

      // Categorical frequencies
      const colFreq = (freqMap[col] ??= {});
      colFreq[value] = (colFreq[value] || 0) + 1;
    });
  });

  /* ---------- Build analysis content ---------- */
  const lines: string[] = [];
  const push = (s = "") => lines.push(s);

  push("分析結果 (Frequency / Bin Analysis):\n");

  // Numeric histograms
  Object.entries(numericData).forEach(([col, nums]) => {
    if (!nums.length) return;
    const min = Math.min(...nums);
    const max = Math.max(...nums);
    const width = (max - min) / DEFAULT_BIN_COUNT || 1;
    const bins = new Array(DEFAULT_BIN_COUNT).fill(0) as number[];

    nums.forEach((v) => {
      let idx = Math.floor((v - min) / width);
      if (idx === DEFAULT_BIN_COUNT) idx = DEFAULT_BIN_COUNT - 1;
      bins[idx] += 1;
    });

    push(`${col}: ${DEFAULT_BIN_COUNT}`);
    bins.forEach((cnt, i) => {
      const start = min + i * width;
      const end = i === DEFAULT_BIN_COUNT - 1 ? max : start + width;
      push(`\t${formatNum(start)}~${formatNum(end)}: ${cnt}`);
    });
    push();
  });

  // Categorical & date-year frequencies
  Object.entries(freqMap).forEach(([col, valueMap]) => {
    const distinct = Object.keys(valueMap).length;
    push(`${col}: ${distinct}`);
    Object.entries(valueMap)
      .sort((a, b) =>
        b[1] - a[1] !== 0 ? b[1] - a[1] : a[0].localeCompare(b[0], "zh-Hant")
      )
      .forEach(([val, count]) => push(`\t${val}: ${count}`));
    push();
  });

  /* ---------- Write to file ---------- */
  try {
    fs.writeFileSync(outputPath, lines.join("\n"), "utf8");
    console.log(`✅  Analysis saved to ${outputPath}`);
  } catch (err) {
    console.error(`❌  Failed to write ${outputPath}`, err);
  }
}

main();
