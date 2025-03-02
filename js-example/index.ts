import { BSON } from "bson";

const serialized = BSON.serialize({ a: ["b"] });

for (const byte of serialized) {
  Bun.write(Bun.stdout, "\\x" + byte.toString(16).padStart(2, "0"));
}
