/**
 * ========================================================================== *
 *  File        : crawl.ts                                                    *
 *  Purpose     : Crawl quarterly MOI ZIP archives, keep Taipei rent tables,  *
 *                and assemble the first raw dataset.                         *
 *                                                                            *
 *  Usage       : ts-node crawl.ts                                            *
 *                                                                            *
 *  Workflow    : 1) Build season list 101S1-current                          *
 *                2) Download & unzip → a_*_c.csv files                       *
 *                3) Apply Stage-1 row filters (住宅用途, valid dates, …)      *
 *                4) Deduplicate on 編號+租賃日+門牌                           *
 *                5) Write rent_ori.csv                                       *
 *                                                                            *
 *  Source file : MOI open-data ZIPs (remote)                                 *
 *  Export file : /root/dataset/rent_ori.csv                                  *
 *  Simple rules: see rule.md §Stage 1                                        *
 *  Updated     : 2025-05-26                                                  *
 * ========================================================================== *
 */

import fs from "node:fs";
import path from "node:path";
import axios from "axios";
import unzipper from "unzipper";
import { parse } from "csv-parse/sync";

const CITY_CODE = "a"; // Taipei City
const EXCLUDE_SET = new Set(["土地", "車位", "建物"]);
const ZONING_RESIDENTIAL = /住/; // human-living zoning
const tmpDir = path.resolve("tmp");
const datasetDir = path.resolve("dataset");

fs.mkdirSync(tmpDir, { recursive: true });
fs.mkdirSync(datasetDir, { recursive: true });

/* ---------- helpers ------------------------------------------------ */

function generateSeasons(): string[] {
  const seasons: string[] = [];
  const CURRENT_ROC_YEAR = 114; // 2025-05-13 = ROC 114
  const CURRENT_SEASON = 1; // last finished quarter
  for (let y = 101; y <= CURRENT_ROC_YEAR; y++) {
    const lastQ = y === CURRENT_ROC_YEAR ? CURRENT_SEASON : 4;
    for (let s = 1; s <= lastQ; s++) seasons.push(`${y}S${s}`);
  }
  return seasons;
}

async function downloadZip(url: string, outfile: string) {
  console.log("⇣ downloading zip …", path.basename(outfile));
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

/* ---------- row parser / filters ---------------------------------- */

interface Row {
  [k: string]: string;
}

function filterRows(csvPath: string, source: string): Row[] {
  const csv = fs.readFileSync(csvPath, "utf8");

  const records: Row[] = parse(csv, {
    columns: (hdr) => hdr.map((h: string) => h.replace(/^\uFEFF/, "").trim()),
    skip_empty_lines: true,
    trim: true,
    relax_quotes: true,
    relax_column_count: true,
    skip_records_with_error: true,
  });

  if (records.length <= 1) return []; // Chinese + English header only
  const dataRows = records.slice(1); // drop English header

  return dataRows
    .filter((r) => {
      /* 1) 交易標的  ---------------------------------------------- */
      if (EXCLUDE_SET.has((r["交易標的"] ?? "").trim())) return false;

      /* 2) 土地使用分區  ------------------------------------------ */
      const zu = r["都市土地使用分區"] ?? "";
      const zr = r["非都市土地使用分區"] ?? "";
      if (!(ZONING_RESIDENTIAL.test(zu + zr) || (zu === "" && zr === "")))
        return false;

      /* 3) 租賃欄位  --------------------------------------------- */
      const hasRent =
        (r["租賃年月日"] ?? "") !== "" || (r["租賃期間"] ?? "") !== "";
      if (!hasRent) return false;

      return true;
    })
    .map((r) => ({ ...r, source_file: source }));
}

/* ---------- main --------------------------------------------------- */

(async () => {
  const all: Row[] = [];

  /* ––– download + parse every season ––– */
  for (const season of generateSeasons()) {
    const url = `https://plvr.land.moi.gov.tw/DownloadSeason?fileName=lvr_landcsv.zip&season=${season}&type=zip`;
    const zipPath = path.join(tmpDir, `lvr_${season}.zip`);
    const seasonDir = path.join(tmpDir, season); // unique folder per season
    fs.mkdirSync(seasonDir, { recursive: true });

    await downloadZip(url, zipPath);
    await extractZip(zipPath, seasonDir);

    /* read only  a_*_c.csv  files (land-rent detail) */
    const files = fs
      .readdirSync(seasonDir)
      .filter(
        (f) =>
          f.toLowerCase().startsWith(`${CITY_CODE}_`) && /_c\.csv$/i.test(f)
      );

    for (const f of files) {
      const full = path.join(seasonDir, f);
      console.log(`⇣ parsing ${f} (${season}) …`);
      all.push(...filterRows(full, `${season}/${f}`));
    }
  }

  /* -------- dedupe on  編號-交易年月日-門牌 ----------------------- */
  const uniq = Object.values(
    all.reduce((acc, r) => {
      const key = `${r["編號"] ?? ""}-${r["交易年月日"]}-${
        r["土地位置建物門牌"] ?? r["土地區段位置或建物區門牌"]
      }`;
      acc[key] = r;
      return acc;
    }, {} as Record<string, Row>)
  );

  console.log(`✓ ${uniq.length} unique Taipei rent rows`);

  if (uniq.length === 0) {
    console.log("⚠️  No rent rows after filtering – nothing written.");
    return;
  }

  /* -------- align header (union of all keys) --------------------- */
  const header = Array.from(
    uniq.reduce((s, r) => {
      Object.keys(r).forEach((k) => s.add(k));
      return s;
    }, new Set<string>())
  );

  /* -------- write aligned CSV ------------------------------------ */
  const body = uniq
    .map((r) =>
      header.map((k) => `"${String(r[k] ?? "").replace(/"/g, '""')}"`).join(",")
    )
    .join("\n");

  fs.writeFileSync(
    path.join(datasetDir, "rent_ori.csv"),
    header.join(",") + "\n" + body,
    "utf8"
  );

  console.log("\nArtifacts ready:\n  • dataset/taipei_rent.csv");
})();
