#!/usr/bin/env node

import fs from "fs/promises";
import path from "path";

async function inlineWasm() {
  const wasmPath = path.join(process.cwd(), "src", "albedo.wasm");
  const distPath = path.join(process.cwd(), "dist");

  // Read the WASM file
  const wasmBuffer = await fs.readFile(wasmPath);
  const base64 = wasmBuffer.toString("base64");

  console.log(`WASM file size: ${wasmBuffer.length} bytes`);
  console.log(`Base64 size: ${base64.length} bytes`);

  // Create inline version of albedo-wasm.js
  const albedoWasmPath = path.join(distPath, "albedo-wasm.js");
  const originalContent = await fs.readFile(albedoWasmPath, "utf-8");

  // Replace the WASM loading logic with inline base64
  const inlineWasmCode = `
// Inline WASM data (${wasmBuffer.length} bytes)
const INLINE_WASM_DATA = "${base64}";

async function loadWasmBytes(url) {
  // Return the inline WASM data instead of loading from URL
  const binaryString = atob(INLINE_WASM_DATA);
  const bytes = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }
  return bytes.buffer;
}

async function compileWasmModule(url) {
  const bytes = await loadWasmBytes(url);
  wasmModule = await WebAssembly.compile(bytes);
      wasmInstance = await WebAssembly.instantiate(wasmModule, {
        env: envImports,
      });
  return wasmModule;
}

await compileWasmModule(wasmUrl);
`;

  // Replace the loadWasmBytes and compileWasmModule functions
  const modifiedContent = originalContent
    .replace(/async function loadWasmBytes[\s\S]*?^}/m, "")
    .replace(
      /async function compileWasmModule[\s\S]*?^}/m,
      inlineWasmCode.trim() // Remove the original compileWasmModule since it's included in inlineWasmCode
    );

  // Write the inline version
  const inlinePath = path.join(distPath, "albedo-wasm-inline.js");
  await fs.writeFile(inlinePath, modifiedContent);

  // Also create a corresponding .d.ts file
  const dtsPath = path.join(distPath, "albedo-wasm.d.ts");
  const inlineDtsPath = path.join(distPath, "albedo-wasm-inline.d.ts");
  if (
    await fs
      .access(dtsPath)
      .then(() => true)
      .catch(() => false)
  ) {
    const dtsContent = await fs.readFile(dtsPath, "utf-8");
    await fs.writeFile(inlineDtsPath, dtsContent);
  }

  console.log(`Created inline version: ${inlinePath}`);
  console.log(`Inline bundle size: ${modifiedContent.length} bytes`);
}

inlineWasm().catch(console.error);
