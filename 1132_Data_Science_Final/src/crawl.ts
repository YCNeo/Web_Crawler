import fs from "node:fs";
import path from "node:path";
import axios from "axios";
import unzipper from "unzipper";
import { parse } from "csv-parse/sync";

const ZIP_URL =
  "https://plvr.land.moi.gov.tw/Download?fileName=lvr_landcsv.zip&type=zip";
const CITY_CODE = "a"; // Taipei City files start with a_
const EXCLUDE_SET = new Set(["土地", "車位", "建物"]); // 交易標的 to drop
const ZONING_RESIDENTIAL = /住/; // “住” = human-living zoning

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

function filterRows(csvPath: string, fileName: string): Row[] {
  const csv = fs.readFileSync(csvPath, "utf8");

  const records: Row[] = parse(csv, {
    columns: (hdr) => hdr.map((h: string) => h.replace(/^\uFEFF/, "").trim()),
    skip_empty_lines: true,
    trim: true,
  });

  if (records.length <= 1) return []; // Chinese + English headers only
  const dataRows = records.slice(1); // drop English header

  return dataRows
    .filter((r) => {
      /* ① 交易標的: 直接排除土地 / 車位 / 建物 -------------------- */
      const useType = (r["交易標的"] ?? "").trim();
      if (EXCLUDE_SET.has(useType)) return false;

      /* ② 都市 / 非都市土地使用分區: 只要包含「住」才算住宅 -------- */
      const zoningUrban = r["都市土地使用分區"] ?? "";
      const zoningRural = r["非都市土地使用分區"] ?? "";
      const zoningStr = zoningUrban + zoningRural;
      if (!ZONING_RESIDENTIAL.test(zoningStr)) return false;

      return true;
    })
    .map((r) => ({ ...r, source_file: fileName }));
}

(async () => {
  const tmpDir = path.resolve("tmp");
  const datasetDir = path.resolve("dataset");
  const zipFile = path.join(tmpDir, "lvr.zip");
  fs.mkdirSync(tmpDir, { recursive: true });
  fs.mkdirSync(datasetDir, { recursive: true });

  // await downloadZip(ZIP_URL, zipFile);
  // await extractZip(zipFile, tmpDir);

  // pick every Taipei City file inside the zip
  const taipeiFiles = fs.readdirSync(tmpDir).filter(
    (f) =>
      f.toLowerCase().startsWith(`${CITY_CODE}_`) && /_[abc]\.csv$/i.test(f) // only _a.csv _b.csv _c.csv
  );
  console.log(`Found ${taipeiFiles.length} Taipei-City CSVs`);

  const all: Row[] = [];
  for (const file of taipeiFiles) {
    const fullPath = path.join(tmpDir, file);
    console.log(`⇣ parsing ${file} …`);
    all.push(...filterRows(fullPath, file));
  }

  // dedupe  (編號-交易年月日-門牌)
  const uniq = Object.values(
    all.reduce((acc, r) => {
      const key = `${r["編號"] ?? ""}-${r["交易年月日"]}-${
        r["土地區段位置或建物區門牌"]
      }`;
      acc[key] = r;
      return acc;
    }, {} as Record<string, Row>)
  );

  console.log(`✓ ${uniq.length} unique Taipei rows (土地/車位 removed)`);

  /* ---------- split by 租賃 ---------- */
  const isRent = (row: Row) =>
    (row["租賃年月日"] ?? "") !== "" || (row["租賃期間"] ?? "") !== "";

  const rentRows = uniq.filter(isRent);
  const otherRows = uniq.filter((r) => !isRent(r));

  console.log(
    `✓ ${rentRows.length} 租賃 rows  •  ${otherRows.length} 非租賃 rows`
  );

  if (uniq.length === 0) {
    console.log("⚠️  No rows left after filtering – nothing written.");
    return;
  }

  /* ---------- exporter helper ---------- */
  function dumpCSV(rows: Row[], outfile: string) {
    if (rows.length === 0) return;
    const header = Object.keys(rows[0]).join(",");
    const body = rows
      .map((r) =>
        Object.values(r)
          .map((v) => `"${String(v ?? "").replace(/"/g, '""')}"`)
          .join(",")
      )
      .join("\n");
    fs.writeFileSync(outfile, `${header}\n${body}`, "utf8");
  }

  /* ---------- write files ---------- */
  dumpCSV(rentRows, path.join(datasetDir, "taipei_rent.csv"));
  dumpCSV(otherRows, path.join(datasetDir, "taipei_other.csv"));

  console.log(
    "\nArtifacts ready:\n" +
      "  • dataset/taipei_rent.csv\n" +
      "  • dataset/taipei_other.csv"
  );
})();
