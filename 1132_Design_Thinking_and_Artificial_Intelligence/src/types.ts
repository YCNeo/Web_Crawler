export type Nature = "寒" | "涼" | "平" | "溫" | "熱";

export interface RawRow {
  zh: string;
  nature: Nature;
  source: string;
}

export interface MergedRow {
  zh: string;
  nature: Nature;
  variants?: Nature[];
  sources: string[];
}
