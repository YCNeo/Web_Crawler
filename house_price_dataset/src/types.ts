import { REASONS } from "./clean";

/* -------- types --------*/
export type Reason = (typeof REASONS)[number];

export type RemoveRow = Record<Reason, number>;

export type CleanRow = Record<string, unknown>;

export type RawRow = Record<string, string>;

/* -------- interface --------*/
export interface BuildingTypeMap {
  [type: string]: string[];
}

export interface MainUsageMap {
  [type: string]: string[];
}
