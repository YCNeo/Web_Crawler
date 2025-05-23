import dayjs from "dayjs";

const DAY_MS = 86_400_000;

/* -------- interface --------*/
interface BuildingTypeMap {
  [type: string]: string[];
}

interface MainUsageMap {
  [type: string]: string[];
}

export function parseROC(raw: string): dayjs.Dayjs | null {
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

export const rocToISO = (s: string): string | null =>
  parseROC(s)?.format("YYYY-MM-DD") ?? null;

export const periodToDays = (s: string): number | null => {
  const [a, b] = s.split("~").map((x) => parseROC(x)?.valueOf() ?? null);
  return a !== null && b !== null ? Math.round((b - a) / DAY_MS) : null;
};

/** 接受字串或數字並轉為數字；失敗回 null */
export const toNum = (v: unknown): number | null => {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string") {
    const n = parseFloat(v.replace(/,/g, ""));
    return Number.isFinite(n) ? n : null;
  }
  return null;
};

/** 解析「土地/建物/車位」比棟數字串 */
export function parseTransRatio(raw: string | undefined) {
  let land = 0,
    building = 0,
    parking = 0;
  if (!raw) return { land, building, parking };

  // 帶詞的情況
  const pairRE =
    /土地\s*[:：]?\s*(\d+(?:\.\d+)?)|建物\s*[:：]?\s*(\d+(?:\.\d+)?)|車位\s*[:：]?\s*(\d+(?:\.\d+)?)/g;
  let m: RegExpExecArray | null;
  while ((m = pairRE.exec(raw)) !== null) {
    if (m[1] !== undefined) land = parseFloat(m[1]);
    if (m[2] !== undefined) building = parseFloat(m[2]);
    if (m[3] !== undefined) parking = parseFloat(m[3]);
  }

  // 單純數字備援 (ex. "1 1 0")
  if (land === 0 && building === 0 && parking === 0) {
    const nums = raw.match(/\d+(?:\.\d+)?/g);
    if (nums && nums.length >= 3) {
      land = parseFloat(nums[0]);
      building = parseFloat(nums[1]);
      parking = parseFloat(nums[2]);
    }
  }
  return { land, building, parking };
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

export const floorChange = (chinese: string): string => {
  if (chinese === "全" || chinese === "整棟" || chinese === "整層")
    return "透天厝";

  const floor = floorToNumber(chinese);

  if (floor < 0) return "地下室";
  if (floor <= 10) return "低樓層";
  if (floor <= 20) return "中樓層";
  if (floor > 20) return "高樓層";

  return "NA"; // 其他情況
};

export const purposeClassify = (purpose: string): string => {
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
};

export const buildingMaterialClassify = (material: string): string => {
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
};

export const houseAgeClassify = (age: number): string => {
  if (age < 0) return "NA";
  if (age <= 5) return "新屋";
  if (age <= 10) return "5-10年";
  if (age <= 20) return "10-20年";
  if (age <= 30) return "20-30年";
  if (age <= 40) return "30-40年";
  if (age > 40) return "40年以上";

  return "NA"; // 其他情況
};
