import path from "path";
import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  turbopack: {
    root: __dirname, // force project root to icelanddb-app

  },
};

export default nextConfig;
