import axios from "axios";
import { load } from "cheerio";
import { Nature, RawRow } from "../types";

export async function crawlCloudTCM(): Promise<RawRow[]> {
  const UA = { "User-Agent": "OneShotFoodNatureCrawler/0.2" };

  // 1. 先抓 herb 首頁
  const listHtml = await axios
    .get("https://cloudtcm.com/herb", { headers: UA })
    .then((r) => r.data);

  const $ = load(listHtml);

  // 2. 取所有卡片連結
  const links = $('a[href^="/herb/"]')
    .map((_, el) => "https://cloudtcm.com" + $(el).attr("href"))
    .get();

  const rows: RawRow[] = [];

  // 3. 逐頁抓取詳細資料
  for (const url of links) {
    const detailHtml = await axios
      .get(url, { headers: UA })
      .then((r) => r.data);
    const $$ = load(detailHtml);

    const zh = $$(".title").first().text().trim(); // 中文名
    const natureText = $$('li:contains("寒熱指數")').text(); // e.g. "寒熱指數：溫"
    const nature: Nature = natureText.includes("熱")
      ? "hot"
      : natureText.includes("溫")
      ? "warm"
      : natureText.includes("平")
      ? "neutral"
      : natureText.includes("涼")
      ? "cool"
      : "cold";

    rows.push({ zh, nature, source: "cloudtcm-html" });
  }

  return rows;
}

console.log(await crawlCloudTCM().then((r) => r.length));
