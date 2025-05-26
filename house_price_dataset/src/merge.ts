/**
 * ========================================================================== *
 *  File        : merge.ts                                                    *
 *  Purpose     : Enrich cleaned rent data with MRT proximity information.    *
 *                                                                            *
 *  Usage       : ts-node merge.ts                                            *
 *                                                                            *
 *  Workflow    : 1) Left-join rent_cln ← mrt on 編號                         *
 *                2) Copy + rename MRT cols; derive 捷運線, 轉乘站 flag        *
 *                3) Drop rows without 附近建物單位成交均價                     *
 *                4) Remove 編號 and save rent_mrg.csv                        *
 *                                                                            *
 *  Source files: /root/dataset/rent_cln.csv, /root/dataset/mrt.csv           *
 *  Export file : /root/dataset/rent_mrg.csv                                  *
 *  Simple rules: keep rows with MRT price index; rename 距離欄位              *
 *  Updated     : 2025-05-26                                                  *
 * ========================================================================== *
 */

import fs from "node:fs";
import path from "node:path";
import csvParser from "csv-parser";
import { createObjectCsvWriter } from "csv-writer";

type Row = Record<string, string | number>;

function loadCsv(fp: string): Promise<Row[]> {
  return new Promise((res, rej) => {
    const rows: Row[] = [];
    fs.createReadStream(fp)
      .pipe(csvParser())
      .on("data", (r) => rows.push(r))
      .on("end", () => res(rows))
      .on("error", rej);
  });
}

async function main() {
  const datasetDir = path.resolve(__dirname, "../dataset");
  const rentCsvPath = path.join(datasetDir, "rent_cln.csv");
  const mrtCsvPath = path.join(datasetDir, "mrt.csv");
  const outputCsvPath = path.join(datasetDir, "rent_mrg.csv");

  const lineCols = [
    "文湖線",
    "淡水信義線",
    "新北投支線",
    "松山新店線",
    "小碧潭支線",
    "中和新蘆線",
    "板南線",
    "環狀線",
  ];

  const mrtCols = [
    "x",
    "y",
    "最近捷運站",
    "捷運站距離.公尺.",
    ...lineCols,
    "通車日期",
    "附近建物單位成交均價",
  ];

  // Load both CSVs concurrently
  const [rentRows, mrtRows] = await Promise.all([
    loadCsv(rentCsvPath),
    loadCsv(mrtCsvPath),
  ]);

  // Build lookup for MRT rows by 編號 (trimmed)
  const mrtMap = new Map<string, Row>();
  mrtRows.forEach((r) => mrtMap.set(String(r["編號"]).trim(), r));

  const joined: Row[] = rentRows.map((rent) => {
    const id = String(rent["編號"]).trim();
    const mrt = mrtMap.get(id);
    const row: Row = { ...rent };

    // Copy MRT columns
    mrtCols.forEach((c) => {
      row[c] = mrt?.[c] ?? "";
    });

    // Compose 捷運線 and 轉乘站
    const activeLines = lineCols.filter((c) => {
      const v = String(row[c] ?? "").trim();
      return v !== "" && !isNaN(Number(v)) && Number(v) === 1;
    });
    row["捷運線"] = activeLines.join(",");
    row["轉乘站"] = activeLines.length >= 2 ? 1 : 0;

    // Rename coordinate / distance columns
    if ("x" in row) {
      row["座標 x"] = row["x"];
      delete row["x"];
    }
    if ("y" in row) {
      row["座標 y"] = row["y"];
      delete row["y"];
    }
    if ("捷運站距離.公尺." in row) {
      row["捷運站距離(公尺)"] = row["捷運站距離.公尺."];
      delete row["捷運站距離.公尺."];
    }

    return row;
  });

  // Filter out rows with empty price
  const filtered = joined.filter(
    (r) => String(r["附近建物單位成交均價"]).trim() !== ""
  );

  // Remove 編號 column
  filtered.forEach((r) => delete r["編號"]);

  // Dynamically derive header order from the first record
  const headers = Object.keys(filtered[0] || {});

  const writer = createObjectCsvWriter({
    path: outputCsvPath,
    header: headers.map((id) => ({ id, title: id })),
    encoding: "utf8",
  });

  await writer.writeRecords(filtered);
  console.log(`✅ Wrote ${filtered.length} rows → ${outputCsvPath}`);
}

main().catch((e) => {
  console.error("❌ Merge failed:", e);
  process.exit(1);
});
