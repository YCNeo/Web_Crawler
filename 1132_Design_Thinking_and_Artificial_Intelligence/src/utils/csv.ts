import fs from "node:fs";
import { stringify } from "csv-stringify/sync";
import { MergedRow } from "../types";

export function saveCSV(rows: MergedRow[], file = "foods.csv") {
  const csv = stringify(rows, {
    header: true,
    columns: ["zh", "en", "nature", "variants", "sources"],
  });
  fs.writeFileSync(file, csv, "utf8");
}
