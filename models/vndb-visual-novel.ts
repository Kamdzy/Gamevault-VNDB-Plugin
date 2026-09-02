import { VndbExternalLink } from "./vndb-external-link.js";
import { VndbImage } from "./vndb-image.js";
import { VndbProducer } from "./vndb-producer.js";
import { VndbTag } from "./vndb-tag.js";

export interface VndbVisualNovel
{
    released: string;
    image: VndbImage;
    title: string;
    description: string;
    id: string;
    length_minutes: number;
    rating: number;
    screenshots: VndbImage[];
    developers: VndbProducer[];
    tags: VndbTag[];
    devstatus: number;
    extlinks: VndbExternalLink[];
}
