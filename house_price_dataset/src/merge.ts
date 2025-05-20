import fs from "node:fs";
import path from "node:path";
import csvParser from "csv-parser";
import { createObjectCsvWriter } from "csv-writer";

/**
 * Generic record type representing a row in the CSV.
 */
interface CsvRecord {
  [column: string]: string | number;
}

/**
 * Load a CSV file into memory as an array of objects where each key is a column name.
 */
async function loadCsv(filePath: string): Promise<CsvRecord[]> {
  return new Promise((resolve, reject) => {
    const rows: CsvRecord[] = [];
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

async function main() {
  // ─── File Paths ────────────────────────────────────────────────────────────
  const datasetDir = path.resolve(__dirname, "../dataset");
  const rentCsvPath = path.join(datasetDir, "taipei_rent_clean.csv");
  const mrtCsvPath = path.join(datasetDir, "clean_mrt.csv");
  const outputCsvPath = path.join(datasetDir, "rent.csv");

  // ─── Columns To Merge From MRT Dataset ─────────────────────────────────────
  const extraCols = [
    "x",
    "y",
    "最近捷運站",
    "捷運站距離.公尺.",
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

  // ─── Load Both CSV Files ──────────────────────────────────────────────────
  const [rentRows, mrtRows] = await Promise.all([
    loadCsv(rentCsvPath),
    loadCsv(mrtCsvPath),
  ]);

  // Build a lookup Map for MRT rows keyed by "編號"
  const mrtMap = new Map<string, CsvRecord>();
  mrtRows.forEach((row) => {
    const id = String(row["編號"]).trim();
    mrtMap.set(id, row);
  });

  // ─── Left‑Join On "編號"─────────────────────────────────────────────────────
  const mergedRows: CsvRecord[] = rentRows.map((rentRow) => {
    const id = String(rentRow["編號"]).trim();
    const mrtRow = mrtMap.get(id);
    const merged: CsvRecord = { ...rentRow };

    extraCols.forEach((col) => {
      merged[col] = mrtRow?.[col] ?? ""; // keep empty string if MRT value missing
    });

    return merged;
  });

  // ─── Prepare CSV Writer ────────────────────────────────────────────────────
  const header = [
    ...Object.keys(rentRows[0]),
    ...extraCols.filter(
      (c) => !Object.prototype.hasOwnProperty.call(rentRows[0], c)
    ),
  ];

  const csvWriter = createObjectCsvWriter({
    path: outputCsvPath,
    header: header.map((id) => ({ id, title: id })),
    encoding: "utf8",
  });

  await csvWriter.writeRecords(mergedRows);
  console.log(`✅ Merged dataset saved to ${outputCsvPath}`);
}

// Run the script
main().catch((err) => {
  console.error("❌ Merge failed:", err);
  process.exit(1);
});
