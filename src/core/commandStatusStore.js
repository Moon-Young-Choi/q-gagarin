const fs = require("node:fs/promises");
const path = require("node:path");

function safeCommandId(commandId) {
  const value = String(commandId || "");

  if (!/^[a-f0-9-]{16,}$/i.test(value)) {
    throw new Error(`Invalid commandId: ${commandId}`);
  }

  return value;
}

class CommandStatusStore {
  constructor(options = {}) {
    const runtimeDir = options.runtimeDir || path.resolve(process.cwd(), "out", "runtime");
    this.commandStatusDir = options.commandStatusDir || path.join(runtimeDir, "command-status");
  }

  filePath(commandId) {
    return path.join(this.commandStatusDir, `${safeCommandId(commandId)}.json`);
  }

  async write(commandId, payload) {
    const record = {
      updatedAt: new Date().toISOString(),
      commandId,
      ...payload,
    };

    await fs.mkdir(this.commandStatusDir, { recursive: true });
    await fs.writeFile(this.filePath(commandId), `${JSON.stringify(record, null, 2)}\n`);
    return record;
  }

  async read(commandId) {
    try {
      return JSON.parse(await fs.readFile(this.filePath(commandId), "utf8"));
    } catch (error) {
      if (error.code === "ENOENT") {
        return null;
      }

      throw error;
    }
  }
}

module.exports = {
  CommandStatusStore,
};
