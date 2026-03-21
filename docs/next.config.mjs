import { createMDX } from "fumadocs-mdx/next";

const withMDX = createMDX();

/** @type {import('next').NextConfig} */
const config = {
  output: "export",
  basePath: process.env.GITHUB_PAGES ? "/omnihedron" : "",
  images: { unoptimized: true },
};

export default withMDX(config);
