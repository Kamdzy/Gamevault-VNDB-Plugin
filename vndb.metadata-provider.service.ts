import { Injectable } from "@nestjs/common";
import { DeveloperMetadata } from "src/modules/metadata/developers/developer.metadata.entity";
import { GameMetadata } from "src/modules/metadata/games/game.metadata.entity";
import { MinimalGameMetadataDto } from "src/modules/metadata/games/minimal-game.metadata.dto";
import { GenreMetadata } from "src/modules/metadata/genres/genre.metadata.entity";
import { MetadataProvider } from "src/modules/metadata/providers/abstract.metadata-provider.service";
import { TagMetadata } from "src/modules/metadata/tags/tag.metadata.entity";
import { VndbFilterResponse } from "./models/vndb-filter-response";
import { VndbVisualNovel } from "./models/vndb-visual-novel";

@Injectable()
export class VndbMetadataProviderService extends MetadataProvider {
  enabled = true;
  readonly slug = "vndb";
  readonly name = "VNDB";
  readonly priority = 20;

  // VNDB API Rate Limits: 200 requests per 5 minutes, 1 second execution time per minute
  private readonly MAX_REQUESTS_PER_5_MIN = 200;
  private readonly RATE_LIMIT_WINDOW = 5 * 60 * 1000; // 5 minutes in ms
  private readonly MIN_REQUEST_INTERVAL = 1500; // 1.5 seconds between requests to be safe
  private requestTimestamps: number[] = []; // Sliding window of request timestamps
  private lastRequestTime = 0;

  readonly fieldsToInclude = [
    "*",
    "age_ratings.*",
    "cover.*",
    "genres.*",
    "involved_companies.*",
    "involved_companies.company.*",
    "keywords.*",
    "screenshots.*",
    "artworks.*",
    "videos.*",
    "themes.*",
    "websites.*",
  ];

  public override async search(
    query: string
  ): Promise<MinimalGameMetadataDto[]> {
    // Enforce rate limiting with internal sleep (won't return early)
    await this.enforceRateLimit();

    let url = "https://api.vndb.org/kana/vn";
    const data = {
      filters: ["search", "=", query],
      fields:
        "title, image.url, released, length_minutes, description, devstatus, rating, screenshots.url, developers.name, tags.name, tags.id, extlinks.url",
      results: 25,
    };

    let response: Response;
    try {
      response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "User-Agent": "Gamevault-VNDB-Plugin/1.0",
        },
        body: JSON.stringify(data),
      });
    } catch (error) {
      this.logger.error("Failed to fetch from VNDB API", error);
      throw new Error("Failed to fetch from VNDB API - network error");
    }

    // Check for rate limiting BEFORE trying to parse JSON
    if (response.status === 429) {
      const retryAfter = response.headers.get("Retry-After");
      const waitSeconds = retryAfter ? parseInt(retryAfter) : 60;
      this.logger.warn(
        `VNDB API rate limit hit despite precautions. Sleeping ${waitSeconds} seconds...`
      );
      await this.delay(waitSeconds * 1000);
      // Retry the request after waiting
      return this.search(query);
    }

    // Check for other error statuses BEFORE parsing JSON
    if (!response.ok) {
      const errorText = await response.text();
      this.logger.error(
        `VNDB API Error! status: ${
          response.status
        }, response: ${errorText.substring(0, 200)}`
      );
      throw new Error(
        `VNDB API returned error status ${response.status}: ${errorText.substring(0, 200)}`
      );
    }

    // Now it's safe to parse JSON
    let responseData: VndbFilterResponse;
    try {
      const textResponse = await response.text();

      // Check if response starts with HTML (rate limit page)
      if (textResponse.trim().startsWith("<")) {
        this.logger.error(
          "VNDB returned HTML instead of JSON (likely rate limited)"
        );
        this.logger.warn("Sleeping 60 seconds before retry...");
        await this.delay(60000);
        // Retry the request after waiting
        return this.search(query);
      }

      responseData = JSON.parse(textResponse) as VndbFilterResponse;
    } catch (error) {
      this.logger.error("Failed to parse VNDB API response as JSON", error);
      throw new Error("VNDB API returned invalid JSON response");
    }

    try {
      // Filter out incomplete records BEFORE mapping
      const validGames = (responseData.results || []).filter(
        (game) => game && game.title && game.image?.url
      );

      if (validGames.length === 0) {
        // this.logger.warn(`No valid VNDB results found for query: ${query}`);
        // Return empty array only when legitimately no results found (not an error)
        return [];
      }

      const minimalGameMetadata: MinimalGameMetadataDto[] = [];
      for (const result of validGames) {
        const mapped = await this.mapMinimalGameMetadata(
          result as VndbVisualNovel
        );
        if (mapped) {
          minimalGameMetadata.push(mapped);
        }
      }

      // Clear large objects from memory
      responseData = null as any;

      return minimalGameMetadata;
    } catch (error) {
      this.logger.error("Failed to process VNDB search results", error);
      throw new Error("Failed to process VNDB search results");
    }
  }

  public override async getByProviderDataIdOrFail(
    provider_data_id: string
  ): Promise<GameMetadata> {
    // Enforce rate limiting with internal sleep (won't throw early)
    await this.enforceRateLimit();

    const data = {
      filters: ["id", "=", provider_data_id],
      fields:
        "title, image.url, released, length_minutes, description, devstatus, rating, screenshots.url, developers.name, tags.name, tags.id, extlinks.url",
    };
    let url = "https://api.vndb.org/kana/vn";

    let response: Response;
    try {
      response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "User-Agent": "Gamevault-VNDB-Plugin/1.0",
        },
        body: JSON.stringify(data),
      });
    } catch (error) {
      this.logger.error("Failed to fetch from VNDB API", error);
      throw new Error("Failed to fetch from VNDB API - network error");
    }

    // Check for rate limiting BEFORE trying to parse JSON
    if (response.status === 429) {
      const retryAfter = response.headers.get("Retry-After");
      const waitSeconds = retryAfter ? parseInt(retryAfter) : 60;
      this.logger.warn(
        `VNDB API rate limit hit despite precautions. Sleeping ${waitSeconds} seconds...`
      );
      await this.delay(waitSeconds * 1000);
      // Retry the request after waiting
      return this.getByProviderDataIdOrFail(provider_data_id);
    }

    // Check for other error statuses BEFORE parsing JSON
    if (!response.ok) {
      const errorText = await response.text();
      this.logger.error(
        `VNDB API Error! status: ${
          response.status
        }, response: ${errorText.substring(0, 200)}`
      );
      throw new Error(
        `VNDB API Error! status: ${
          response.status
        }, message: ${errorText.substring(0, 200)}`
      );
    }

    // Now it's safe to parse JSON
    let responseData: VndbFilterResponse;
    try {
      const textResponse = await response.text();

      // Check if response starts with HTML (rate limit page)
      if (textResponse.trim().startsWith("<")) {
        this.logger.error(
          "VNDB returned HTML instead of JSON (likely rate limited)"
        );
        this.logger.warn("Sleeping 60 seconds before retry...");
        await this.delay(60000);
        // Retry the request after waiting
        return this.getByProviderDataIdOrFail(provider_data_id);
      }

      responseData = JSON.parse(textResponse) as VndbFilterResponse;
    } catch (error) {
      this.logger.error("Failed to parse VNDB API response as JSON", error);
      throw new Error("VNDB API returned invalid JSON response");
    }

    if (!responseData.results || responseData.results.length === 0) {
      this.logger.warn(`No visual novel found with ID: ${provider_data_id}`);
      throw new Error(`No visual novel found with ID: ${provider_data_id}`);
    }

    try {
      const result = await this.mapGameMetadata(
        responseData.results[0] as VndbVisualNovel
      );

      // Clear response from memory
      responseData = null as any;

      return result;
    } catch (error) {
      this.logger.error("Failed to map VNDB game metadata", error);
      throw error;
    }
  }

  private async mapGameMetadata(
    visualNovel: VndbVisualNovel
  ): Promise<GameMetadata> {
    return {
      age_rating: 99,
      provider_slug: this.slug,
      provider_data_id: visualNovel.id?.toString(),
      provider_data_url: "https://vndb.org/" + visualNovel.id?.toString(),
      title: visualNovel.title,
      release_date:
        visualNovel.released && !isNaN(new Date(visualNovel.released).getTime())
          ? new Date(visualNovel.released)
          : undefined,
      description: visualNovel.description,
      rating: visualNovel.rating,
      url_websites: visualNovel.extlinks?.map((links) => links.url) || [],
      early_access: visualNovel.devstatus === 1,
      url_screenshots:
        visualNovel.screenshots?.map((screenshot) => screenshot.url) || [],
      url_trailers: undefined,
      url_gameplays: undefined,
      average_playtime: visualNovel.length_minutes,
      developers:
        visualNovel.developers?.map(
          (developer) =>
            ({
              provider_slug: this.slug,
              provider_data_id: developer.id,
              name: developer.name,
            } as DeveloperMetadata)
        ) || [],
      publishers: [],
      genres: [
        {
          provider_slug: this.slug,
          provider_data_id: "1",
          name: "Visual Novel",
        } as GenreMetadata,
      ],
      tags:
        visualNovel.tags?.map(
          (tag) =>
            ({
              provider_slug: this.slug,
              provider_data_id: tag.id,
              name: tag.name,
            } as TagMetadata)
        ) || [],
      cover: await this.downloadImage(visualNovel.image?.url),
      background: undefined,
    } as GameMetadata;
  }

  private async mapMinimalGameMetadata(
    game: VndbVisualNovel
  ): Promise<MinimalGameMetadataDto | null> {
    if (!game) {
      this.logger.warn("Attempted to map null/undefined VNDB game data");
      return null;
    }

    if (!game.image || !game.image.url) {
      this.logger.warn(
        `VNDB game missing image data: ${game.title || "Unknown"}`
      );
      return null;
    }

    if (!game.title) {
      this.logger.warn("VNDB game missing title, skipping");
      return null;
    }

    return {
      provider_slug: this.slug,
      provider_data_id: game.id?.toString(),
      title: game.title,
      description: game.description || undefined,
      release_date:
        game.released && !isNaN(new Date(game.released).getTime())
          ? new Date(game.released)
          : undefined,
      cover_url: game.image.url,
    } as MinimalGameMetadataDto;
  }

  private async downloadImage(url?: string) {
    if (!url) return undefined;
    try {
      return await this.mediaService.downloadByUrl(url);
    } catch (error) {
      this.logger.error(`Failed to download image from ${url}:`, error);
      return undefined;
    }
  }

  /**
   * Enforce VNDB API rate limiting (200 requests per 5 minutes)
   * Uses a sliding window approach and sleeps internally if needed
   */
  private async enforceRateLimit(): Promise<void> {
    const now = Date.now();

    // Clean up old timestamps outside the 5-minute window
    this.requestTimestamps = this.requestTimestamps.filter(
      (timestamp) => now - timestamp < this.RATE_LIMIT_WINDOW
    );

    // If we've hit the limit, calculate how long to wait
    if (this.requestTimestamps.length >= this.MAX_REQUESTS_PER_5_MIN) {
      const oldestRequest = this.requestTimestamps[0];
      const waitTime = this.RATE_LIMIT_WINDOW - (now - oldestRequest) + 100; // +100ms buffer
      
      if (waitTime > 0) {
        this.logger.warn(
          `Rate limit approaching (${this.requestTimestamps.length}/${this.MAX_REQUESTS_PER_5_MIN} requests). ` +
          `Sleeping ${Math.ceil(waitTime / 1000)} seconds...`
        );
        await this.delay(waitTime);
      }
    }

    // Also enforce minimum interval between requests (1.5 seconds)
    const timeSinceLastRequest = now - this.lastRequestTime;
    if (timeSinceLastRequest < this.MIN_REQUEST_INTERVAL) {
      const delayNeeded = this.MIN_REQUEST_INTERVAL - timeSinceLastRequest;
      await this.delay(delayNeeded);
    }

    // Record this request
    this.requestTimestamps.push(Date.now());
    this.lastRequestTime = Date.now();
  }

  /**
   * Delay execution for specified milliseconds
   */
  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}