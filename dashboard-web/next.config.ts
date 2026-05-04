import type { NextConfig } from "next";
import path from "path";
import { fileURLToPath } from "url";

const appDir = path.dirname(fileURLToPath(import.meta.url));

const nextConfig: NextConfig = {
  // Avoid wrong workspace root when other lockfiles exist (e.g. home or monorepo parent).
  turbopack: {
    root: appDir,
  },
};

export default nextConfig;
