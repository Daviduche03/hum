import { mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

const local = (name) => fileURLToPath(new URL(name, import.meta.url));

mkdirSync(local("./public/"), { recursive: true });

await build({
  entryPoints: [local("./app.tsx")],
  outfile: local("./public/client.js"),
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  jsx: "automatic",
  sourcemap: true,
  define: { "process.env.NODE_ENV": '"development"' },
  logLevel: "info",
});
