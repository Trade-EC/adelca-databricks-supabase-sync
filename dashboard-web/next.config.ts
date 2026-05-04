import type { NextConfig } from "next";
import path from "path";
import { fileURLToPath } from "url";

const appDir = path.dirname(fileURLToPath(import.meta.url));

const nextConfig: NextConfig = {
  // Avoid wrong workspace root when other lockfiles exist (e.g. home or monorepo parent).
  turbopack: {
    root: appDir,
  },
  /**
   * Vercel serverless + Turbopack: bundling @aws-sdk/* pulls ESM-only subdeps and fails at runtime
   * with ERR_REQUIRE_ESM (@nodable/entities from xml-builder). Load SDK from node_modules instead.
   */
  serverExternalPackages: [
    "@aws-sdk/client-s3",
    "@aws-sdk/client-lambda",
    "@aws-sdk/client-cloudwatch-logs",
  ],
};

export default nextConfig;
