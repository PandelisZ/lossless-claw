// Build native exports from this fork; no application checkout or external source cache.
import { build } from "esbuild";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
export const root = path.dirname(fileURLToPath(import.meta.url));
export const commit = "51c3c90bfe82fa649e4895dd1a2c4874b5e28f76";
export async function prepare(source = process.env.LCM_UPSTREAM_SOURCE) {
  if (source) {
    // An explicitly supplied checkout must still match the frozen reference.
    execFileSync("git", ["-C", source, "diff", "--exit-code", commit, "--", "src", "index.ts"], {stdio:"pipe"});
  } else {
    // Native development in this fork must not change the comparison baseline.
    source=path.join(root,".generated","sqlite-baseline-source");
    await mkdir(source,{recursive:true});
    const archive=execFileSync("git",["-C",path.resolve(root,"../.."),"archive",commit,"src","index.ts","package.json"],{maxBuffer:32*1024*1024});
    execFileSync("tar",["-xf","-","-C",source],{input:archive,stdio:["pipe","pipe","pipe"]});
  }
  const metadata = JSON.parse(
    await readFile(path.join(source, "package.json")),
  );
  if (metadata.version !== "1.2.0")
    throw new Error("Unexpected upstream version");
  await mkdir(path.join(root, ".generated"), { recursive: true });
  const exports = {
    "src/db/connection.ts": [
      "createLcmDatabaseConnection",
      "closeLcmConnection",
    ],
    "src/db/migration.ts": ["runLcmMigrations"],
    "src/store/conversation-store.ts": [
      "ConversationStore",
      "normalizeMessageContentForFullTextIndex",
    ],
    "src/openclaw-sender-metadata.ts": [
      "serializeOpenClawSenderMetadata",
      "parseOpenClawSenderMetadata",
    ],
    "src/store/parse-utc-timestamp.ts": ["parseUtcTimestamp"],
    "src/store/message-identity.ts": ["buildMessageIdentityHash"],
    "src/store/summary-store.ts": ["SummaryStore"],
    "src/store/fts5-sanitize.ts": ["sanitizeFts5Query"],
    "src/store/full-text-fallback.ts": [
      "buildLikeSearchPlan",
      "containsCjk",
      "createFallbackSnippet",
    ],
    "src/store/search-regex.ts": ["compileSafeSearchRegex"],
    "src/assembler.ts": ["ContextAssembler"],
    "src/retrieval.ts": ["RetrievalEngine"],
    "src/compaction.ts": ["CompactionEngine"],
  };
  await build({
    stdin: {
      contents: Object.entries(exports)
        .map(
          ([file, names]) =>
            `export {${names.join(",")}} from ${JSON.stringify(path.join(source, file))}`,
        )
        .join("\n"),
      resolveDir: root,
    },
    bundle: true,
    platform: "node",
    format: "esm",
    target: "node26",
    external: ["openclaw", "openclaw/*", "@earendil-works/*"],
    outfile: path.join(root, ".generated/upstream.mjs"),
  });
  await writeFile(
    path.join(root, ".generated/provenance.json"),
    JSON.stringify({ version: metadata.version, commit }),
  );
}
if (process.argv[1] === fileURLToPath(import.meta.url)) await prepare();
