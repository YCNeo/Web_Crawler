export const cleanZh = (s: string) =>
  s.replace(/\s+/g, "").replace(/[（(].*?[)）]/g, "");
export const key = cleanZh;

export const mapEn2Zh: Record<string, string> = {
  apple: "蘋果",
  banana: "香蕉",
  pumpkin: "南瓜",
  watermelon: "西瓜",
  ginger: "薑",
};
