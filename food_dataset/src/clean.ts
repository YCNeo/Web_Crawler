import fs from "node:fs";
import path from "node:path";

const src = path.join("../", "dataset", "food_nature.csv");
const out = path.join("../", "dataset", "food_nature_clean.csv");

(async () => {
  const data = fs.readFileSync(src, "utf8");
  const lines = data.split("\n");

  for (let i = 0; i < lines.length; i++) {
    const row = lines[i].split(",");
    const cleaned = row.map((item) => {
      if (item === "熱性") return "熱";
      if (item === "溫性") return "溫";
      if (item === "平性") return "平";
      if (item === "涼性") return "涼";
      if (item === "寒性") return "寒";
      return item;
    });
    lines[i] = cleaned.join(",");
  }

  fs.writeFileSync(out, lines.join("\n"), "utf8");
})();
