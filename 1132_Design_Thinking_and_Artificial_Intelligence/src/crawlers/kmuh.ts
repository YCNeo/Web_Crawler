import axios from "axios";
import { load } from "cheerio";
import { RawRow, Nature } from "../types";

/** KMUH〈中醫之食物屬性飲食指導〉主表爬蟲 */
export async function crawlKMUH(): Promise<RawRow[]> {
  const html = await axios
    .get("https://www.kmuh.org.tw/www/kmcj/data/11009/20.htm", {
      headers: { "User-Agent": "FoodNatureBot/0.5" },
    })
    .then((r) => r.data as string);

  const $ = load(html);
  const rows: RawRow[] = [];

  const trans: Record<string, Nature> = {
    寒涼性: "寒",
    平和性: "平",
    溫熱性: "溫",
  };

  const clean = (s: string) => s.replace(/[\s\u3000\xa0]/g, ""); // 去換行、全形空白、NBSP

  $("table")
    .first()
    .find("tr")
    .each((_, tr) => {
      const tds = $(tr).find("td");
      if (tds.length < 3) return;

      let attrRaw = tds.eq(1).text();
      let foodsRaw = tds.eq(2).text();

      let attr = clean(attrRaw);
      let foods = foodsRaw.trim();

      // 如果第 2 欄空，就到第 3 欄開頭找屬性
      if (!attr) {
        const foodsTrim = foods.replace(/^[\s\u3000\xa0]+/, ""); // 去掉前導 NBSP/空格
        const m = foodsTrim.match(/^(寒涼性|平和性|溫熱性)/);
        if (m) {
          attr = m[1];
          foods = foodsTrim.slice(m[1].length).trim();
        }
      }

      const nature = trans[attr];
      if (!nature || !foods) return;

      foods
        .split(/[、,，\s]+/)
        .map((s) => s.trim())
        .filter(Boolean)
        .forEach((food) => {
          rows.push({ zh: food, nature, source: "kmuh" });
        });
    });

  console.log("kmuh rows", rows.length); // 應約 184
  return rows;
}

/* 單檔測試：pnpm exec tsx src/crawlers/kmuh.ts */
if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const n = (await crawlKMUH()).length;
    console.log("✅ kmuh rows", n);
  })();
}
