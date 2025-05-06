import { load } from "cheerio";
import { get } from "../utils/fetch";
import { RawRow, Nature } from "../types";

/**
 * 療日子 HealingDaily ─ 寒涼性食物專文
 * URL: https://www.healingdaily.com.tw/articles/寒涼性食物-保健補充/
 * 主要表格：食材 | 屬性
 */
export async function crawlHealing(): Promise<RawRow[]> {
  const html = await get(
    "https://www.healingdaily.com.tw/articles/%E5%AF%92%E6%B6%BC%E6%80%A7%E9%A3%9F%E7%89%A9-%E4%BF%9D%E5%81%A5%E8%A3%9C%E5%85%85/"
  );
  const $ = load(html);

  const label2nature: Record<string, Nature> = {
    寒: "cold",
    涼: "cool",
    平: "neutral",
    溫: "warm",
    熱: "hot",
  };

  const rows: RawRow[] = [];

  $("table tbody tr").each((_, tr) => {
    const tds = $(tr).find("td");
    const zh = tds.eq(0).text().trim();
    const label = tds.eq(1).text().trim();

    if (zh && label2nature[label]) {
      rows.push({
        zh,
        nature: label2nature[label],
        source: "healing",
      });
    }
  });

  return rows;
}
