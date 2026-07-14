import { readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const docsRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const source = await readFile(join(docsRoot, "..", "openapi.yaml"), "utf8");
const localServer = [
  "servers:",
  "    - description: Local llmgw",
  "      url: http://localhost:8080",
  "",
].join("\n");
const serverBlock = /^servers:\n(?:(?: {4}|\t).*?(?:\n|$))+/m;
const documentationSpec = serverBlock.test(source)
  ? source.replace(serverBlock, localServer)
  : localServer + source;

const destination = join(docsRoot, "openapi.yaml");
const temporary = join(
  docsRoot,
  `.openapi.yaml.${process.pid}.${Date.now()}.tmp`,
);
try {
  await writeFile(temporary, documentationSpec);
  await rename(temporary, destination);
} finally {
  await rm(temporary, { force: true });
}
