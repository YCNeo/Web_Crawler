import fs from "node:fs";
import path from "node:path";
import { stringify } from "csv-stringify/sync";

// ---------- 檔案路徑 ----------
const src = path.resolve("assets", "food_5_property.pdf");
const out = "pdf_foods.csv";

// ---------- 行首屬性 → 五性 ----------
const prop2nature: [RegExp, "熱" | "溫" | "平" | "涼" | "寒"][] = [
  [/^熱性/, "熱"],
  [/^溫熱性|^溫性/, "溫"],
  [/^平和性|^平性/, "平"],
  [/^涼性/, "涼"],
  [/^寒涼性|^寒性/, "寒"],
];

(async () => {
  // ① 讀檔
  const data = new Uint8Array(fs.readFileSync(src));

  // ② 動態 import pdfjs-dist，真正函式在 .default
  const pdfjs = (await import("pdfjs-dist/legacy/build/pdf.js")).default;

  const doc = await pdfjs.getDocument({ data }).promise;
  const page = await doc.getPage(2); // 第 2 頁
  const { items } = await page.getTextContent();

  const lines = (items as any[]).map((i) => i.str.trim()).filter(Boolean);

  // ③ 解析
  const rows: { zh: string; nature: "熱" | "溫" | "平" | "涼" | "寒" }[] = [];

  for (let line of lines) {
    const hit = prop2nature.find(([re]) => re.test(line));
    if (!hit) continue;

    const [re, nature] = hit;
    line = line.replace(re, "").trim(); // 去屬性詞

    line
      .split(/[、,，\s]+/)
      .map((s) => s.trim())
      .filter(Boolean)
      .forEach((zh) => rows.push({ zh, nature }));
  }

  // ④ 輸出 CSV（僅 zh,nature 兩欄）
  fs.writeFileSync(
    out,
    stringify(rows, { header: true, columns: ["zh", "nature"] }),
    "utf8"
  );
  console.log(`✔  轉出 ${rows.length} 筆 → ${out}`);
})();
