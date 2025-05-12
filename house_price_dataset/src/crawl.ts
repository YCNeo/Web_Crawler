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

// Build ["101S1","101S2",…,"114S1"] dynamically
function generateSeasons(): string[] {
  const seasons: string[] = [];
  const CURRENT_ROC_YEAR = 114; // ← 2025-05-13 = ROC 114
  const CURRENT_SEASON = 1; // latest finished quarter
  for (let y = 101; y <= CURRENT_ROC_YEAR; y++) {
    const lastQ = y === CURRENT_ROC_YEAR ? CURRENT_SEASON : 4;
    for (let s = 1; s <= lastQ; s++) seasons.push(`${y}S${s}`);
  }
  return seasons;
}

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
    relax_quotes: true, // ← tolerate bad quoting
    relax_column_count: true, // ← tolerate extra commas in those bad rows
    skip_records_with_error: true, // ← silently drop the malformed line
  });

  if (records.length <= 1) return [];
  const dataRows = records.slice(1); // drop English header

  return dataRows
    .filter((r) => {
      /* ① 交易標的 ----------------------------- */
      const useType = (r["交易標的"] ?? "").trim();
      if (EXCLUDE_SET.has(useType)) return false;

      /* ② 土地使用分區  ------------------------ */
      const zoningUrban = r["都市土地使用分區"] ?? "";
      const zoningRural = r["非都市土地使用分區"] ?? "";
      const zoningStr = zoningUrban + zoningRural;

      const keepZoning =
        ZONING_RESIDENTIAL.test(zoningStr) || // contains「住」
        (zoningUrban === "" && zoningRural === ""); // or both blank

      if (!keepZoning) return false;

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
  const all: Row[] = [];

  /* ---------- fetch ALL seasons into tmp/ ---------- */
  const seasons = generateSeasons();
  console.log(`⇣ downloading ${seasons.length} quarterly archives …`);

  for (const season of generateSeasons()) {
    const url = `https://plvr.land.moi.gov.tw/DownloadSeason?fileName=lvr_landcsv.zip&season=${season}&type=zip`;
    const zipPath = path.join(tmpDir, `lvr_${season}.zip`);
    const seasonDir = path.join(tmpDir, season); // ← ① unique folder
    fs.mkdirSync(seasonDir, { recursive: true });

    await downloadZip(url, zipPath);
    await extractZip(zipPath, seasonDir); // ← ② extract there

    /* ---- now parse just this season's files & push into `all` ---- */
    const files = fs
      .readdirSync(seasonDir)
      .filter(
        (f) =>
          f.toLowerCase().startsWith(`${CITY_CODE}_`) && /_[abc]\.csv$/i.test(f)
      );

    for (const file of files) {
      const full = path.join(seasonDir, file);
      console.log(`⇣ parsing ${file} (${season}) …`);
      all.push(...filterRows(full, `${season}/${file}`)); // season in source_file
    }
  }

  // pick every Taipei City file inside the zip
  const taipeiFiles = fs.readdirSync(tmpDir).filter(
    (f) =>
      f.toLowerCase().startsWith(`${CITY_CODE}_`) && /_[abc]\.csv$/i.test(f) // keep only _a/_b/_c core tables
  );
  console.log(
    `Found ${taipeiFiles.length} Taipei-City CSVs across all seasons`
  );

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
