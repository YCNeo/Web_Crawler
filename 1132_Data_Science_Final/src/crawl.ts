import fs from "node:fs";
import path from "node:path";
import axios from "axios";
import unzipper from "unzipper";
import { parse } from "csv-parse/sync";

const ZIP_URL =
  "https://plvr.land.moi.gov.tw/Download?fileName=lvr_landcsv.zip&type=zip";
const CITY_CODE = "a"; // 'a_' files ⇒ Taipei City
const DISTRICT = "文山區"; // filter key – change if needed

async function downloadZip(url: string, outfile: string) {
  console.log("⇣ downloading zip …");
  const res = await axios.get(url, {
    responseType: "arraybuffer",
    timeout: 30_000,
  });
  fs.writeFileSync(outfile, res.data);
}

async function extractZip(zipPath: string, outDir: string) {
  await fs
    .createReadStream(zipPath)
    .pipe(unzipper.Extract({ path: outDir }))
    .promise();
}

interface Row {
  [k: string]: string;
}

function hasDistrictColumn(rec: Row): rec is Row & { 鄉鎮市區: string } {
  return Object.prototype.hasOwnProperty.call(rec, "鄉鎮市區");
}

function filterRows(csvPath: string): Row[] {
  const csv = fs.readFileSync(csvPath, "utf8");

  const records: Row[] = parse(csv, {
    // ①  normalise every header before the first row is emitted
    columns: (hdr: string[]) => hdr.map((h) => h.replace(/^\uFEFF/, "").trim()),
    skip_empty_lines: true,
    trim: true,
  });

  if (records.length === 0 || !("鄉鎮市區" in records[0])) return [];

  return records.filter((r) => (r["鄉鎮市區"] ?? "").includes(DISTRICT));
}

(async () => {
  const tmpDir = path.resolve("tmp");
  const datasetDir = path.resolve("dataset");
  const zipFile = path.join(tmpDir, "lvr.zip");
  fs.mkdirSync(tmpDir, { recursive: true });
  fs.mkdirSync(datasetDir, { recursive: true });

  await downloadZip(ZIP_URL, zipFile);
  await extractZip(zipFile, tmpDir);

  // --- pick every Taipei City file inside the zip ---
  const taipeiFiles = fs
    .readdirSync(tmpDir)
    .filter(
      (f) => f.toLowerCase().startsWith(`${CITY_CODE}_`) && f.endsWith(".csv")
    );

  console.log(`Found ${taipeiFiles.length} Taipei‑City CSVs`);

  const all: Row[] = [];
  for (const file of taipeiFiles)
    all.push(...filterRows(path.join(tmpDir, file)));

  // --- dedupe (use 統一編號 + 交易年月日 + 地號 as a simple composite key) ---
  const uniq = Object.values(
    all.reduce((acc, r) => {
      const key = `${r["編號"] ?? ""}-${r["交易年月日"]}-${
        r["土地區段位置或建物區門牌"]
      }`;
      acc[key] = r; // last one wins if duplicates
      return acc;
    }, {} as Record<string, Row>)
  );

  console.log(`✓ ${uniq.length} unique rows for 台北市文山區`);

  if (uniq.length === 0) {
    console.log(`⚠️  No rows matched 「${DISTRICT}」 – nothing written.`);
    return;
  }

  // --- export ---
  fs.writeFileSync(
    `${datasetDir}/wenshan.json`,
    JSON.stringify(uniq, null, 2),
    "utf8"
  );

  const header = Object.keys(uniq[0]).join(",");
  const csvBody = uniq
    .map((r) =>
      Object.values(r)
        .map((s) => `"${String(s).replace(/"/g, '""')}"`)
        .join(",")
    )
    .join("\n");
  fs.writeFileSync(
    `${datasetDir}/wenshan.csv`,
    `${header}\n${csvBody}`,
    "utf8"
  );

  console.log("\nArtifacts ready:\n  • wenshan.json\n  • wenshan.csv");
})();
