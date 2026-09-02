
import { Module } from "@nestjs/common";

import {
  GameVaultPluginModule,
  GameVaultPluginModuleMetadataV1,
} from "../../../src/globals.js";
import { MetadataModule } from "../../../src/modules/metadata/metadata.module.js";
import { MediaModule } from "../../../src/modules/media/media.module.js";
import { VndbMetadataProviderService } from "./vndb.metadata-provider.service.js";

@Module({
  imports: [
    MetadataModule,
    MediaModule
  ],
  providers: [VndbMetadataProviderService],
})
export default class VndbMetadataProviderPluginModule implements GameVaultPluginModule {
  metadata: GameVaultPluginModuleMetadataV1 = {
    name: "VNDB Metadata Provider",
    author: "nosyrbllewe",
    version: "1.0.0",
    description:
      "Adds VNDB as a metadata provider for GameVault",
    keywords: ["metadata", "provider", "vndb", "visual novels"],
    license: "UNLICENSE",
    website: "",
  };
}