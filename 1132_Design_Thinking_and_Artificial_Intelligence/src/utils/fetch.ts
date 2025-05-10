import axios from "axios";

export const UA = { "User-Agent": "OneShotFoodNatureCrawler/0.2" };

export async function get(url: string) {
  return axios
    .get<string>(url, { headers: UA, timeout: 15000 })
    .then((r) => r.data);
}
