// ===========================================================
//  NCCU CIS 多學期課程代碼爬蟲  (Node.js 18+)
//  - 讀取環境變數 CIS_COOKIE  (必填)
//  - 讀取環境變數 CIS_SEMESTERS，如未設就 fallback 為 ['1141']
//  - 將每學期 → 全院系 → program 一次展開，依 code 去重
//  - 產生 deptCodes.js  (ES module，可直接 import)
// ===========================================================

import fs from 'fs/promises';

// --------------  0. 讀取環境變數  ----------------
const COOKIE = process.env.CIS_COOKIE;
const SEMESTERS = (process.env.CIS_SEMESTERS || '1141')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);

if (!COOKIE) {
    console.error('❌  請先 export CIS_COOKIE="..."'); process.exit(1);
}

console.log(`🔎 學期清單：${SEMESTERS.join(', ')}`);

// 共用 fetch helper
const headers = { Cookie: COOKIE, 'User-Agent': 'NCCU-Crawler/1.1' };
const base = 'https://cis.nccu.edu.tw/course/api';
const fetchJSON = async url => {
    const r = await fetch(url, { headers });
    if (!r.ok) throw new Error(`${r.status} ${url}`);
    return r.json();
};

// ---------- 1. 逐學期抓取並累積到 Map 去重 ----------
const map = new Map();   // key = progCode, value = record

for (const sem of SEMESTERS) {
    console.log(`📥 抓取學期 ${sem} …`);
    const deps = await fetchJSON(`${base}/departments?semester=${sem}`);

    for (const d of deps) {
        const progList = await fetchJSON(
            `${base}/departments/${d.deptNo}/programs?semester=${sem}`
        );

        progList.forEach(p => {
            const key = p.progCode;
            // 若之前已存在，直接略過 (或可自行選擇覆蓋策略)
            if (map.has(key)) return;

            map.set(key, {
                code: p.progCode,             // e.g. "EC2"
                name: p.progName,             // 中文全名
                degree: p.degree,               // B / M / D / EMBA / …
                group: p.groupName ?? '',      // 甲組 / 乙組 / ""
                college: d.collegeName,
                dept: d.deptName,
                english: p.language === 'E',     // true = 全英語課程
                semesterFound: sem               // 第一次出現的學期
            });
        });
    }
}

const records = [...map.values()];
console.log(`✅ 完成：共 ${records.length} 筆 (含大學部 + 碩博 + 專班)`);

// ---------- 2. 輸出為 ES module ----------
const js = `// Auto-generated via fetchNccuDeptCodes.mjs
// Semesters: ${SEMESTERS.join(', ')}

export const deptCodes = ${JSON.stringify(records, null, 2)};
`;
await fs.writeFile('deptCodes.js', js, 'utf8');
console.log('💾 已寫入 deptCodes.js');

// ---------- 3. 若只想留研究所(含在職) ----------
const grad = records.filter(r => /^(M|D|EMBA|IMBA|In)/i.test(r.degree));
await fs.writeFile(
    'gradDeptCodes.js',
    `export const gradDeptCodes = ${JSON.stringify(grad, null, 2)};\n`,
    'utf8'
);
console.log(`📂 gradDeptCodes.js 另存 ${grad.length} 筆 (研究所／專班)`);

