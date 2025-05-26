/**
 * ========================================================================== *
 *  File        : order.ts                                                    *
 *  Purpose     : Apply a canonical column order to every CSV in /dataset.    *
 *                                                                            *
 *  Usage       : ts-node order.ts                                            *
 *                                                                            *
 *  Workflow    : 1) For each CSV                                             *
 *                   – Parse header                                           *
 *                   – Reorder via headerOrder array                          *
 *                   – Overwrite file in place                                *
 *                                                                            *
 *  Source file : Any pipeline CSV in /root/dataset                           *
 *  Export file : Overwrites the same path                                    *
 *  Simple rule : Unlisted columns keep original relative order               *
 *  Updated     : 2025-05-26                                                  *
 * ========================================================================== *
 */

import { promises as fs } from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import { stringify } from "csv-stringify/sync";

const headerOrder: readonly string[] = [
  "編號",
  "鄉鎮市區",
  "土地位置建物門牌",
  "租賃年月日",
  "總額元",
  "出租型態",
  "租賃天數",
  "主要用途",
  "主要用途分類",
  "租賃層次",
  "租賃層次分類",
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
  "主要建材分類",
  "建物現況格局-房",
  "建物現況格局-廳",
  "建物現況格局-衛",
  "建物現況格局-隔間",
  "建物總面積平方公尺",
  "單價元平方公尺",
  "建築完成年月",
  "屋齡",
  "屋齡分類",
  "附屬設備-冷氣",
  "附屬設備-熱水器",
  "附屬設備-洗衣機",
  "附屬設備-電視機",
  "附屬設備-冰箱",
  "附屬設備-瓦斯或天然氣",
  "附屬設備-有線電視",
  "附屬設備-網路",
  "座標 x",
  "座標 y",
  "最近捷運站",
  "捷運站距離(公尺)",
  "捷運線",
  "轉乘站",
  "文湖線",
  "淡水信義線",
  "新北投支線",
  "松山新店線",
  "小碧潭支線",
  "中和新蘆線",
  "板南線",
  "環狀線",
  "通車日期",
  "附近建物單位成交均價",
];

// ────────────────────────────────────────────────────────────
// Paths
// ────────────────────────────────────────────────────────────
const datasetDir = path.resolve(__dirname, "../dataset");

/**
 * Produce a header array that respects the CSV's own columns while bringing
 * any recognised columns to the front in the preferred order.
 */
function deriveHeader(original: string[]): string[] {
  const preferred = headerOrder.filter((h) => original.includes(h));
  const remainder = original.filter((h) => !headerOrder.includes(h));
  return [...preferred, ...remainder];
}

async function reorderCsv(filePath: string) {
  const raw = await fs.readFile(filePath, "utf8");
  const records = parse(raw, {
    columns: true,
    skip_empty_lines: true,
  }) as Record<string, string>[];

  if (records.length === 0) {
    // Empty file – just copy as‑is to maintain pipeline integrity.
    await fs.mkdir(path.dirname(filePath), { recursive: true });
    await fs.writeFile(filePath, raw, "utf8");
    return;
  }

  const originalHeader = Object.keys(records[0]);
  const orderedHeader = deriveHeader(originalHeader);

  // Re‑shape each row according to orderedHeader **without** introducing new keys.
  const reshaped = records.map((row) => {
    const newRow: Record<string, string> = {};
    for (const col of orderedHeader) {
      newRow[col] = row[col];
    }
    return newRow;
  });

  const csv = stringify(reshaped, { header: true, columns: orderedHeader });
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, csv, "utf8");
}

async function main() {
  const entries = await fs.readdir(datasetDir);
  const csvFiles = entries.filter((n) => n.toLowerCase().endsWith(".csv"));

  if (csvFiles.length === 0) {
    console.warn("No CSV files found in", datasetDir);
    return;
  }

  console.log(`Found ${csvFiles.length} CSV file(s). Processing…`);

  for (const name of csvFiles) {
    const filePath = path.join(datasetDir, name);
    await reorderCsv(filePath); // overwrite in place
    console.log(`✔ ${name} (overwritten)`);
  }

  console.log("All done! Files overwritten in", datasetDir);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
