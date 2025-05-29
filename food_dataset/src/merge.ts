import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import csvParser from "csv-parser";
import { createObjectCsvWriter } from "csv-writer";

/** ------------------------------------------------------------------------
 * merge.ts – Left-join two CSV files on Chinese food names, harmonise
 *            食材屬性 with 五性, then output only English/Chinese/FiveNature.
 *
 *  INPUT 1 : ../dataset/checklist_with_properties.csv  (key col: 中文)
 *  INPUT 2 : ../dataset/food_nature_clean.csv         (key col: 食物名稱)
 *  OUTPUT  : ../dataset/merged.csv
 * -------------------------------------------------------------------------*/

type Row = Record<string, string | number>;

/** Load CSV → array of objects, stripping BOM + trimming cells */
function loadCsv(fp: string): Promise<Row[]> {
  return new Promise((resolve, reject) => {
    const rows: Row[] = [];
    fs.createReadStream(fp)
      .pipe(csvParser())
      .on("data", (r) => rows.push(r))
      .on("end", () => {
        const cleaned = rows.map((row) => {
          const out: Row = {};
          for (const k of Object.keys(row)) {
            const key = k.replace(/^\uFEFF/, "").trim();
            const val = row[k];
            out[key] = typeof val === "string" ? val.trim() : val;
          }
          return out;
        });
        resolve(cleaned);
      })
      .on("error", reject);
  });
}

async function main() {
  /* Resolve dataset directory relative to this script */
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const datasetDir = path.resolve(__dirname, "../dataset");

  const leftPath = path.join(datasetDir, "checklist_with_properties.csv");
  const rightPath = path.join(datasetDir, "food_nature_clean.csv");
  const outPath = path.join(datasetDir, "merged.csv");

  const [leftRows, rightRows] = await Promise.all([
    loadCsv(leftPath),
    loadCsv(rightPath),
  ]);
  if (!leftRows.length) throw new Error("Left CSV is empty");

  /* Build lookup map 食物名稱 → 五性 */
  const rightMap = new Map<string, string>();
  rightRows.forEach((r) => {
    const key = String(r["食物名稱"] ?? "").trim();
    if (key) rightMap.set(key, String(r["五性"] ?? "").trim());
  });

  /* Merge, harmonise attributes, and transform columns */
  const finalRows: Row[] = leftRows.map((left) => {
    const key = String(left["中文"] ?? "").trim();
    const wuXing = rightMap.get(key) ?? "";

    // 1) Clean 食材屬性 – remove trailing 『性』
    let attr = String(left["食材屬性"] ?? "")
      .trim()
      .replace(/性$/, "");

    // 2) If 五性 present and differs, adopt 五性
    if (wuXing && attr !== wuXing) attr = wuXing;

    /* Build output record with renamed / reordered columns */
    return {
      English: left["英文"] ?? "",
      Chinese: key,
      FiveNature: attr,
    } as Row;
  });

  /* Prepare writer with desired header order */
  const headers = [
    { id: "English", title: "English" },
    { id: "Chinese", title: "Chinese" },
    { id: "FiveNature", title: "FiveNature" },
  ];

  const writer = createObjectCsvWriter({
    path: outPath,
    header: headers,
    encoding: "utf8",
  });

  await writer.writeRecords(finalRows);
  console.log(`✅ Wrote ${finalRows.length} rows → ${outPath}`);
}

main().catch((err) => {
  console.error("❌ Merge failed:", err);
  process.exit(1);
});
