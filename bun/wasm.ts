import { join } from "path";
const albedo_wasm = await Bun.file(
  join(__dirname, "../zig-out/bin/albedo.wasm")
).arrayBuffer();
const module = await WebAssembly.compile(albedo_wasm);

const imports = {
  wasm_open_file: () => {},
  wasm_close_file: () => {},
  wasm_pread: () => {},
  wasm_pwrite: () => {},
  wasm_sync: () => {},
  wasm_delete_file: () => {},
  wasm_rename_file: () => {},
  wasm_random_bytes: () => {},
  wasm_now_seconds: () => {},
};
const instance = await WebAssembly.instantiate(module, { env: imports });

console.log("Albedo WASM module loaded:", instance.exports);
