import { VndbVisualNovel } from "./vndb-visual-novel.js";

export interface VndbFilterResponse 
{
    results : VndbVisualNovel[];
    more: boolean;
}