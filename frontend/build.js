// Frontend build: bundle the page (core engine + IndexedDB adapter + UI)
// into a single app.js and copy the static assets to the output directory.
// Usage: node build.js [--out <dir>]   (default: ./dist)

import { build } from "esbuild";
import { copyFile, mkdir, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const outFlag = process.argv.indexOf("--out");
const outDir =
  outFlag !== -1
    ? path.resolve(process.argv[outFlag + 1])
    : path.join(here, "dist");

await rm(outDir, { recursive: true, force: true });
await mkdir(outDir, { recursive: true });

await build({
  entryPoints: [path.join(here, "src", "main.js")],
  bundle: true,
  format: "iife",
  target: ["es2022"],
  platform: "browser",
  outfile: path.join(outDir, "app.js"),
  logLevel: "warning",
});

for (const file of ["index.html", "styles.css"]) {
  await copyFile(path.join(here, "src", file), path.join(outDir, file));
}

console.log(`frontend built -> ${outDir}`);
