import { load } from "cheerio";
import { get } from "../utils/fetch";
import { RawRow, Nature } from "../types";

/**
 * Ping Ming Health 文章
 * URL: https://www.pingminghealth.com/article/581/warming-and-cooling-characteristics-of-common-foods/
 * 結構：三個 <h3> (Cooling / Warming / Neutral) 各接一個 <ul><li>
 */
export async function crawlPingMing(): Promise<RawRow[]> {
  const html = await get(
    "https://www.pingminghealth.com/article/581/warming-and-cooling-characteristics-of-common-foods/"
  );
  const $ = load(html);

  const map: Record<string, Nature> = {
    Cooling: "cool",
    Warming: "warm",
    Neutral: "neutral",
  };

  const rows: RawRow[] = [];

  $("h3").each((_, h) => {
    const heading = $(h).text().trim();
    const nature = map[heading as keyof typeof map];
    if (!nature) return;

    $(h)
      .nextUntil("h3", "ul")
      .find("li")
      .each((_, li) => {
        const en = $(li).text().trim().toLowerCase();
        rows.push({
          zh: "", // 稍後用 mapEn2Zh 補中文
          en,
          nature,
          source: "pingming",
        });
      });
  });

  return rows;
}
