import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { mkdir, open, lstat, realpath } from "node:fs/promises";
import path from "node:path";
import { partitionSqliteExport, partitionManifest } from "./partition.js";
import { exportHash, type SqliteExport } from "./transfer.js";

const digest = (value: string | Buffer) =>
  createHash("sha256").update(value).digest("hex");
type Payload = {
  fileId: string;
  relativePath: string;
  bytes: number;
  sha256: string;
};
type AgentBundle = {
  agentId: string;
  exportFile: string;
  exportHash: string;
  sourceHash: string;
  rows: number;
  largeFilesDir: string;
  files: Payload[];
  rewrites: { fileId: string; from: string; to: string }[];
};
export type MigrationBundle = {
  version: 1;
  organizationId: string;
  sourceHash: string;
  agents: AgentBundle[];
};
const within = (root: string, target: string) =>
  target.startsWith(root + path.sep) && target !== root;
/** Reject symlink components, traversal and non-regular payloads; never follow a source out of its root. */
async function safeFile(root: string, target: string) {
  if (!path.isAbsolute(target) || !within(root, path.resolve(target)))
    throw new Error("Payload path is outside its declared root");
  let current = root;
  for (const part of path.relative(root, target).split(path.sep)) {
    current = path.join(current, part);
    const info = await lstat(current);
    if (info.isSymbolicLink())
      throw new Error("Symlink payload components are unsupported");
  }
  if (process.platform !== "linux")
    throw new Error(
      "Verified migration payload opens currently require Linux procfs"
    );
  const file = await open(target, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const opened = await realpath(`/proc/self/fd/${file.fd}`);
    if (!within(root, opened))
      throw new Error(
        "Opened payload escaped its declared root during a namespace change"
      );
    const info = await file.stat({ bigint: true });
    if (!info.isFile()) throw new Error("Payload must be a regular file");
    return { file, info };
  } catch (error) {
    await file.close();
    throw error;
  }
}
async function payloadHash(root: string, target: string) {
  const { file, info } = await safeFile(root, target);
  const hash = createHash("sha256");
  let bytes = 0;
  try {
    for await (const chunk of file.createReadStream({ autoClose: false })) {
      hash.update(chunk);
      bytes += chunk.length;
    }
    const after = await file.stat({ bigint: true });
    if (
      info.ino !== after.ino ||
      info.size !== after.size ||
      info.mtimeNs !== after.mtimeNs ||
      info.ctimeNs !== after.ctimeNs
    )
      throw new Error("Payload changed during verification");
    return { bytes, sha256: hash.digest("hex") };
  } finally {
    await file.close();
  }
}
async function durableWrite(target: string, value: string) {
  const file = await open(target, "wx", 0o600);
  try {
    if ((await realpath(`/proc/self/fd/${file.fd}`)) !== target)
      throw new Error("Migration destination namespace changed");
    await file.writeFile(value);
    await file.sync();
  } finally {
    await file.close();
  }
}
async function syncDirectory(directory: string) {
  const file = await open(
    directory,
    constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW
  );
  try {
    if ((await realpath(`/proc/self/fd/${file.fd}`)) !== directory)
      throw new Error("Migration destination namespace changed");
    await file.sync();
  } finally {
    await file.close();
  }
}
/** Source export is immutable. Output is exclusively created; manifest.json is the completion marker. */
export async function createMigrationBundle(input: {
  data: SqliteExport;
  organizationId: string;
  sourcePayloadRoot: string;
  destination: string;
}) {
  if (!input.organizationId.trim())
    throw new Error("Migration requires an explicit organization identity");
  const parts = partitionSqliteExport(input.data),
    summary = partitionManifest(input.data, parts);
  if (!parts.size) throw new Error("Migration source contains no agents");
  const destination = path.join(
      await realpath(path.dirname(path.resolve(input.destination))),
      path.basename(input.destination)
    ),
    sourceRoot = await realpath(input.sourcePayloadRoot);
  if (destination === sourceRoot || within(sourceRoot, destination))
    throw new Error(
      "Migration bundle must be outside the source payload directory"
    );
  await mkdir(destination, { mode: 0o700 });
  const manifest: MigrationBundle = {
    version: 1,
    organizationId: input.organizationId,
    sourceHash: summary.sourceHash,
    agents: [],
  };
  const payloadOwners = new Map<string, string>();
  for (const [agentId, data] of parts) {
    const directory = path.join(destination, "agents", agentId),
      largeFilesDir = path.join(directory, "files");
    await mkdir(largeFilesDir, { recursive: true, mode: 0o700 });
    const agent: AgentBundle = {
      agentId,
      exportFile: `agents/${agentId}/export.json`,
      exportHash: "",
      sourceHash: exportHash(data),
      rows: data.tables.reduce((n, t) => n + t.rows.length, 0),
      largeFilesDir,
      files: [],
      rewrites: [],
    };
    for (const row of data.tables.find((t) => t.name === "large_files")?.rows ??
      []) {
      if (
        typeof row.file_id !== "string" ||
        typeof row.storage_uri !== "string"
      )
        throw new Error("Invalid payload metadata");
      if (!path.isAbsolute(row.storage_uri))
        throw new Error("Migration payload requires an absolute source path");
      const source = path.resolve(row.storage_uri),
        relativePath = `agents/${agentId}/files/${digest(row.file_id)}.payload`,
        target = path.join(destination, relativePath);
      const { file, info } = await safeFile(sourceRoot, source);
      const inode = `${info.dev}:${info.ino}`,
        previousOwner = payloadOwners.get(inode);
      if (previousOwner && previousOwner !== agentId) {
        await file.close();
        throw new Error("Source payload inode is shared across agents");
      }
      payloadOwners.set(inode, agentId);
      let output;
      const hash = createHash("sha256");
      let bytes = 0;
      try {
        output = await open(target, "wx", 0o600);
        if ((await realpath(`/proc/self/fd/${output.fd}`)) !== target)
          throw new Error("Migration destination namespace changed");
        for await (const chunk of file.createReadStream({ autoClose: false })) {
          await output.writeFile(chunk);
          hash.update(chunk);
          bytes += chunk.length;
        }
        await output.sync();
        const after = await file.stat({ bigint: true });
        if (
          info.ino !== after.ino ||
          info.size !== after.size ||
          info.mtimeNs !== after.mtimeNs ||
          info.ctimeNs !== after.ctimeNs
        )
          throw new Error("Payload changed during migration");
      } finally {
        await output?.close();
        await file.close();
      }
      if (
        row.byte_size !== null &&
        row.byte_size !== undefined &&
        row.byte_size !== bytes
      )
        throw new Error("Payload size disagrees with LCM metadata");
      const sha256 = hash.digest("hex"),
        copied = await payloadHash(destination, target);
      if (copied.bytes !== bytes || copied.sha256 !== sha256)
        throw new Error("Copied payload verification failed");
      agent.files.push({ fileId: row.file_id, relativePath, bytes, sha256 });
      agent.rewrites.push({
        fileId: row.file_id,
        from: row.storage_uri,
        to: target,
      });
      row.storage_uri = target;
    }
    agent.exportHash = exportHash(data);
    await durableWrite(
      path.join(destination, agent.exportFile),
      JSON.stringify(data) + "\n"
    );
    manifest.agents.push(agent);
  }
  for (const agent of manifest.agents) {
    await syncDirectory(agent.largeFilesDir);
    await syncDirectory(path.dirname(agent.largeFilesDir));
  }
  await syncDirectory(path.join(destination, "agents"));
  await syncDirectory(destination);
  const encoded = JSON.stringify(manifest, null, 2) + "\n";
  await durableWrite(path.join(destination, "manifest.json"), encoded);
  await syncDirectory(destination);
  await syncDirectory(path.dirname(destination));
  return {
    manifestHash: digest(encoded),
    sourceHash: manifest.sourceHash,
    agents: manifest.agents.map((a) => ({
      agentId: a.agentId,
      rows: a.rows,
      files: a.files.length,
      exportHash: a.exportHash,
    })),
  };
}
export async function verifyMigrationBundle(
  directory: string,
  expectedManifestHash: string,
  organizationId: string
) {
  if (!/^[a-f0-9]{64}$/.test(expectedManifestHash))
    throw new Error(
      "Verification requires the expected manifest SHA256 from preparation"
    );
  const root = await realpath(directory),
    marker = path.join(root, "manifest.json");
  const markerInfo = await safeFile(root, marker);
  let encoded: string;
  try {
    encoded = await markerInfo.file.readFile("utf8");
  } finally {
    await markerInfo.file.close();
  }
  if (digest(encoded) !== expectedManifestHash)
    throw new Error("Migration manifest hash mismatch");
  const manifest = JSON.parse(encoded) as MigrationBundle;
  if (
    manifest.version !== 1 ||
    manifest.organizationId !== organizationId ||
    !Array.isArray(manifest.agents) ||
    !manifest.agents.length
  )
    throw new Error("Migration manifest identity/version mismatch");
  const agents = new Map<string, SqliteExport>();
  for (const agent of manifest.agents) {
    if (
      !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/.test(agent.agentId) ||
      agents.has(agent.agentId) ||
      !Array.isArray(agent.files) ||
      !Array.isArray(agent.rewrites)
    )
      throw new Error("Invalid migration agent inventory");
    const exportPath = path.resolve(root, agent.exportFile);
    const handle = await safeFile(root, exportPath);
    let data: SqliteExport;
    try {
      data = JSON.parse(await handle.file.readFile("utf8"));
    } finally {
      await handle.file.close();
    }
    if (exportHash(data) !== agent.exportHash)
      throw new Error("Migration export hash mismatch");
    const partitioned = partitionSqliteExport(data);
    if (partitioned.size !== 1 || !partitioned.has(agent.agentId))
      throw new Error("Migration export agent mismatch");
    const metadata =
      data.tables.find((t) => t.name === "large_files")?.rows ?? [];
    if (
      agent.files.length !== metadata.length ||
      agent.rewrites.length !== metadata.length
    )
      throw new Error("Migration payload inventory mismatch");
    const seen = new Set<string>();
    for (const payload of agent.files) {
      if (seen.has(payload.fileId))
        throw new Error("Duplicate migration payload");
      seen.add(payload.fileId);
      const target = path.resolve(root, payload.relativePath),
        row = metadata.find((r) => r.file_id === payload.fileId),
        rewrite = agent.rewrites.find((r) => r.fileId === payload.fileId);
      if (
        !row ||
        !rewrite ||
        row.storage_uri !== target ||
        rewrite.to !== target ||
        !within(path.resolve(root, "agents", agent.agentId, "files"), target)
      )
        throw new Error("Migration payload ownership/path mismatch");
      const actual = await payloadHash(root, target);
      if (actual.bytes !== payload.bytes || actual.sha256 !== payload.sha256)
        throw new Error("Migration payload hash mismatch");
    }
    const original = structuredClone(data);
    for (const row of original.tables.find((t) => t.name === "large_files")
      ?.rows ?? [])
      row.storage_uri = agent.rewrites.find(
        (r) => r.fileId === row.file_id
      )!.from;
    if (exportHash(original) !== agent.sourceHash)
      throw new Error("Migration source reconstruction hash mismatch");
    agents.set(agent.agentId, data);
  }
  return { manifest, agents };
}
