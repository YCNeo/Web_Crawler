import dayjs from "dayjs";
import {
  BuildingTypeMap,
  CleanRow,
  MainUsageMap,
  RawRow,
  Reason,
} from "../types";
import {
  DROP_COLS,
  EQUIP_COLS,
  EQUIP_SEP,
  MAX_LAYOUT,
  PURPOSE_RE,
} from "../clean";

const DAY_MS = 86_400_000;

function parseROC(raw: string): dayjs.Dayjs | null {
  let s = (raw ?? "").toString().trim();
  if (!s || s.toLowerCase() === "nan") return null;
  if (s.endsWith(".0")) s = s.slice(0, -2);

  // 6 or 7 digits
  if (/^\d{6,7}$/.test(s)) {
    if (s.length === 6) s = `0${s}`; // 6位補一位
    const yyy = +s.slice(0, 3);
    const mm = +s.slice(3, 5);
    const dd = +s.slice(5, 7);
    const d = dayjs(`${yyy + 1911}-${mm}-${dd}`, "YYYY-M-D", true);
    return d.isValid() ? d : null;
  }

  // 其他含分隔符
  const m = s.match(/(\d{1,3})[./年\-](\d{1,2})[./月\-](\d{1,2})/);
  if (!m) return null;
  const d = dayjs(`${+m[1] + 1911}-${m[2]}-${m[3]}`, "YYYY-M-D", true);
  return d.isValid() ? d : null;
}

function rocToISO(s: string): string | null {
  return parseROC(s)?.format("YYYY-MM-DD") ?? null;
}

function periodToDays(s: string): number | null {
  const [a, b] = s.split("~").map((x) => parseROC(x)?.valueOf() ?? null);
  return a !== null && b !== null ? Math.round((b - a) / DAY_MS) : null;
}

/** 接受字串或數字並轉為數字；失敗回 null */
function toNum(v: unknown): number | null {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string") {
    const n = parseFloat(v.replace(/,/g, ""));
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

/** 解析「土地/建物/車位」比棟數字串 */
export function parseTransRatio(raw: string | undefined): {
  land: number;
  building: number;
} {
  const m = (raw ?? "").match(
    /土地\s*([\d.]+)\s*建物\s*([\d.]+)\s*車位\s*([\d.]+)/
  );

  return {
    land: m ? parseFloat(m[1]) : 0,
    building: m ? parseFloat(m[2]) : 0,
  };
}

function floorToNumber(chinese: string): number {
  const digitMap: Record<string, number> = {
    零: 0,
    一: 1,
    二: 2,
    兩: 2,
    三: 3,
    四: 4,
    五: 5,
    六: 6,
    七: 7,
    八: 8,
    九: 9,
  };

  // 1. Remove trailing "層"
  chinese = chinese.endsWith("層") ? chinese.slice(0, -1) : chinese;

  // 2. Check for "地下" (negative floor)
  const isNegative = chinese.startsWith("地下");
  if (isNegative) {
    chinese = chinese.slice(2);
  }

  // 3. Convert the remaining Chinese number (0–99)
  let result = 0;

  if (chinese.length === 1) {
    result = digitMap[chinese] ?? 0;
  } else {
    const tenIndex = chinese.indexOf("十");
    if (tenIndex !== -1) {
      const tens = tenIndex === 0 ? 1 : digitMap[chinese[tenIndex - 1]] ?? 1;
      result += tens * 10;

      const onesChar = chinese[tenIndex + 1];
      if (onesChar && digitMap[onesChar] !== undefined) {
        result += digitMap[onesChar];
      }
    } else {
      result = digitMap[chinese] ?? 0;
    }
  }

  return isNegative ? -result : result;
}

function floorChange(chinese: string): string {
  if (chinese === "全" || chinese === "整棟" || chinese === "整層")
    return "透天厝";

  const floor = floorToNumber(chinese);

  if (floor < 0) return "地下室";
  if (floor <= 10) return "低樓層";
  if (floor <= 20) return "中樓層";
  if (floor > 20) return "高樓層";

  return "NA"; // 其他情況
}

function purposeClassify(purpose: string): string {
  const mainUsage: MainUsageMap = {
    住宅類: ["住宅", "住家用", "集合住宅", "多戶住宅", "公寓"],
    住商混合: ["住商", "住工", "住宅、店舖"],
    商業用途: ["商業", "辦公", "事務所", "零售業", "店舖"],
    工業用途: ["工業", "工廠", "廠房", "倉儲"],
    特殊用途: ["防空", "醫", "學", "福利", "宿舍", "交通"],
    未知: [""], // 空字串或 NA
    其他: ["其他"], // 任何未被歸類到以上條件的值
  };

  for (const [type, usages] of Object.entries(mainUsage)) {
    if (usages.some((u) => purpose.includes(u))) {
      return type;
    }
  }
  return "NA";
}

function buildingMaterialClassify(material: string): string {
  const buildingType: BuildingTypeMap = {
    鋼筋混凝土造類: [
      "鋼筋混凝土",
      "ＲＣ",
      "鋼骨鋼筋混凝土",
      "鋼骨混凝土",
      "鋼骨ＲＣ造",
    ],
    加強磚造類: [
      "加強磚造",
      "磚造",
      "磚石造",
      "加強石造",
      "石造",
      "土磚石混合造",
    ],
    鋼骨類: ["鋼骨造", "鋼骨", "鋼構造"],
    木造類: ["木", "竹"],
  };

  for (const [type, materials] of Object.entries(buildingType)) {
    if (materials.some((m) => material.includes(m))) {
      return type;
    }
  }

  return "NA";
}

function houseAgeClassify(age: number): string {
  if (age < 0) return "NA";
  if (age <= 5) return "新屋";
  if (age <= 10) return "5-10年";
  if (age <= 20) return "10-20年";
  if (age <= 30) return "20-30年";
  if (age <= 40) return "30-40年";
  if (age > 40) return "40年以上";

  return "NA"; // 其他情況
}

export function getRemovalReason(
  row: RawRow,
  { land, building }: { land: number; building: number }
): Reason | null {
  if (!PURPOSE_RE.test(row["主要用途"] ?? "")) return "用途不符";
  if (row["交易標的"].includes("房地")) return "交易標的不符";
  if (row["車位類別"] !== "") return "無須車位出租相關資訊";
  if (land === 0 && building === 0) return "租賃筆棟數不符";
  if (row["租賃層次"] === "見其他登記事項") return "租賃層次不明";
  if (row["主要建材"] === "見其他登記事項" || row["主要建材"] === "見使用執照")
    return "主要建材不明";
  if (row["單價元平方公尺"] === "") return "單價為零";

  const builtISO = rocToISO(row["建築完成年月"] ?? "");
  if (builtISO === null) return "建築完成年月缺失";

  const leaseISO = rocToISO(row["租賃年月日"] ?? "");
  if (!leaseISO) return "租賃年月日缺失";

  const rooms = +row["建物現況格局-房"]!;
  const halls = +row["建物現況格局-廳"]!;
  const baths = +row["建物現況格局-衛"]!;
  if (rooms > MAX_LAYOUT) return "房過大";
  if (halls > MAX_LAYOUT) return "廳過大";
  if (baths > MAX_LAYOUT) return "衛過大";

  const total = toNum(row["總額元"]);
  if (total === null) return "總額缺失";

  return null;
}

export function transformRow(
  row: RawRow,
  { land, building }: { land: number; building: number }
): CleanRow {
  /* ---------- derived basics ---------- */
  const builtISO = rocToISO(row["建築完成年月"] ?? "")!; // guaranteed valid
  const leaseISO = rocToISO(row["租賃年月日"] ?? "")!;
  const age = dayjs(leaseISO).diff(dayjs(builtISO), "year");

  /* ---------- core output object ---------- */
  const out: CleanRow = {
    租賃年月日: leaseISO,
    建築完成年月: builtISO,
    屋齡: age,
    屋齡分類: houseAgeClassify(age),
    "交易筆棟數-土地": land,
    "交易筆棟數-建物": building,
    租賃天數: periodToDays(row["租賃期間"] ?? "") ?? "NA",
    主要用途: row["主要用途"],
    主要用途分類: purposeClassify(row["主要用途"] ?? ""),
    主要建材: row["主要建材"]?.trim() || "NA",
    主要建材分類: buildingMaterialClassify(row["主要建材"]?.trim() || "NA"),
    "建物現況格局-房": +row["建物現況格局-房"]!,
    "建物現況格局-廳": +row["建物現況格局-廳"]!,
    "建物現況格局-衛": +row["建物現況格局-衛"]!,
    租賃層次: row["租賃層次"],
    租賃層次分類: floorChange(row["租賃層次"]?.trim() || "NA"),
    總額元: toNum(row["總額元"]),
  };

  /* ---------- boolean-like flags ---------- */
  [
    "建物現況格局-隔間",
    "有無管理組織",
    "有無附傢俱",
    "有無電梯",
    "有無管理員",
  ].forEach((k) => {
    const v = row[k]?.trim();
    out[k] = v === "有" ? 1 : v === "無" ? 0 : "NA";
  });

  /* ---------- simple “NA” fills ---------- */
  ["出租型態", "租賃住宅服務"].forEach((k) => {
    const v = row[k]?.trim();
    out[k] = v === "" ? "NA" : v;
  });

  /* ---------- copy untouched columns ---------- */
  for (const [col, v] of Object.entries(row)) {
    if (
      DROP_COLS.has(col) ||
      col === "附屬設備" ||
      col === "租賃期間" ||
      out.hasOwnProperty(col)
    )
      continue;
    out[col] = v;
  }

  /* ---------- expand equipment flags ---------- */
  const ownEquip = new Set(
    (row["附屬設備"] ?? "")
      .split(EQUIP_SEP)
      .map((x) => x.trim())
      .filter(Boolean)
  );

  for (const eq of EQUIP_COLS) {
    out[`附屬設備-${eq}`] = ownEquip.has(eq) ? 1 : 0;
  }

  return out;
}
