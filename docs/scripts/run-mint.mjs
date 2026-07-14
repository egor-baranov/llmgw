import { existsSync } from "node:fs";
import { spawn } from "node:child_process";
import { delimiter, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const scriptsDirectory = dirname(fileURLToPath(import.meta.url));
const docsDirectory = dirname(scriptsDirectory);
const executableName = process.platform === "win32" ? "node.exe" : "node";
const nodeExecutable = join(
  docsDirectory,
  "node_modules",
  "node",
  "bin",
  executableName,
);
const mintEntrypoint = join(
  docsDirectory,
  "node_modules",
  "@mintlify",
  "cli",
  "bin",
  "index.js",
);

if (!existsSync(nodeExecutable) || !existsSync(mintEntrypoint)) {
  console.error("Documentation dependencies are missing. Run `npm ci` in docs/ first.");
  process.exit(1);
}

const environment = {
  ...process.env,
  PATH: [dirname(nodeExecutable), process.env.PATH]
    .filter(Boolean)
    .join(delimiter),
};

const mint = spawn(
  nodeExecutable,
  [mintEntrypoint, ...process.argv.slice(2)],
  {
    cwd: docsDirectory,
    detached: process.platform !== "win32",
    env: environment,
    stdio: "inherit",
  },
);

let stopping = false;
let forceStopTimer;

function stopMint(signal) {
  if (!mint.pid || mint.exitCode !== null || mint.signalCode !== null) {
    return;
  }
  try {
    if (process.platform === "win32") {
      mint.kill(signal);
    } else {
      process.kill(-mint.pid, signal);
    }
  } catch (error) {
    if (error.code !== "ESRCH") {
      throw error;
    }
  }
}

for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  process.on(signal, () => {
    if (stopping) {
      stopMint("SIGKILL");
      return;
    }
    stopping = true;
    stopMint(signal);
    forceStopTimer = setTimeout(() => stopMint("SIGKILL"), 5000);
  });
}

mint.once("error", (error) => {
  console.error(`Unable to start Mintlify: ${error.message}`);
  process.exitCode = 1;
});

mint.once("exit", (code, signal) => {
  clearTimeout(forceStopTimer);
  process.exitCode = code ?? (signal ? 1 : 0);
});

process.once("exit", () => stopMint("SIGTERM"));
