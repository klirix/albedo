// Minimal BSON serializer/deserializer for common types.
// Functions:
//   serialize(obj: any): Uint8Array
//   deserialize(buf: Uint8Array): any

const enum Type {
  Double = 0x01,
  String = 0x02,
  Document = 0x03,
  Array = 0x04,
  Binary = 0x05,
  ObjectId = 0x07,
  Boolean = 0x08,
  DateTime = 0x09,
  Null = 0x0a,
  Regex = 0x0b,
  Int32 = 0x10,
  Int64 = 0x12,
}
const customInspectSymbol = Symbol.for("nodejs.util.inspect.custom");

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export class ObjectId {
  readonly id: Uint8Array;

  constructor(input?: Uint8Array | ArrayLike<number> | string) {
    if (typeof input === "string") {
      const tmp = ObjectId.fromString(input);
      this.id = tmp.id;
    } else if (input && (input as any).length === 12) {
      // copy
      const arr = new Uint8Array(12);
      for (let i = 0; i < 12; i++) arr[i] = (input as ArrayLike<number>)[i]!;
      this.id = arr;
    } else if (input === undefined) {
      // generate: 4 bytes timestamp (big-endian) + 8 random bytes
      const buf = new Uint8Array(12);
      const ts = Math.floor(Date.now() / 1000);
      buf[0] = (ts >>> 24) & 0xff;
      buf[1] = (ts >>> 16) & 0xff;
      buf[2] = (ts >>> 8) & 0xff;
      buf[3] = (ts >>> 0) & 0xff;
      if (
        typeof crypto !== "undefined" &&
        typeof crypto.getRandomValues === "function"
      ) {
        crypto.getRandomValues(buf.subarray(4));
      } else {
        for (let i = 4; i < 12; i++) buf[i] = (Math.random() * 256) | 0;
      }
      this.id = buf;
    } else {
      // fallback: try to fill 12 bytes and pad with zeros
      const arr = new Uint8Array(12);
      const src = input as ArrayLike<number>;
      for (let i = 0; i < Math.min(12, src.length); i++) arr[i] = src[i]!;
      this.id = arr;
    }
  }

  static fromString(hex: string): ObjectId {
    if (hex.length !== 24) throw new Error("ObjectId hex must be 24 hex chars");
    const arr = new Uint8Array(12);
    for (let i = 0; i < 12; i++) {
      const hi = parseInt(hex[2 * i]!, 16);
      const lo = parseInt(hex[2 * i + 1]!, 16);
      if (Number.isNaN(hi) || Number.isNaN(lo))
        throw new Error("Invalid hex string for ObjectId");
      arr[i] = (hi << 4) | lo;
    }
    return new ObjectId(arr);
  }

  toString(): string {
    let s = "";
    for (let i = 0; i < 12; i++) {
      const v = this.id[i]!;
      s += (v >>> 4).toString(16);
      s += (v & 0xf).toString(16);
    }
    return s;
  }

  [customInspectSymbol](
    depth?: number,
    options?: unknown,
    inspect?: (x: unknown, options?: unknown) => string
  ): string {
    inspect ??= defaultInspect;
    return `new ObjectId(${inspect(this.toString(), options)})`;
  }
}

export function defaultInspect(x: unknown, _options?: unknown): string {
  return JSON.stringify(x, (k: string, v: unknown) => {
    if (typeof v === "bigint") {
      return { $numberLong: `${v}` };
    } else if (v instanceof Map) {
      return Object.fromEntries(v);
    }
    return v;
  });
}

class ByteWriter {
  private buffer: ArrayBuffer;
  private view: DataView;
  private bytes: Uint8Array;
  private _length = 0;

  constructor(initialCapacity = 256) {
    this.buffer = new ArrayBuffer(initialCapacity);
    this.view = new DataView(this.buffer);
    this.bytes = new Uint8Array(this.buffer);
  }

  get length(): number {
    return this._length;
  }

  private ensureCapacity(size: number) {
    const required = this._length + size;
    if (required <= this.bytes.length) return;
    let newCapacity = this.bytes.length || 16;
    while (newCapacity < required) {
      newCapacity =
        newCapacity < 1024 ? newCapacity * 2 : newCapacity + (newCapacity >> 1);
    }
    const next = new ArrayBuffer(newCapacity);
    new Uint8Array(next).set(this.bytes.subarray(0, this._length));
    this.buffer = next;
    this.view = new DataView(this.buffer);
    this.bytes = new Uint8Array(this.buffer);
  }

  writeUint8(value: number) {
    this.ensureCapacity(1);
    this.bytes[this._length++] = value & 0xff;
  }

  writeBytes(src: Uint8Array) {
    this.ensureCapacity(src.length);
    this.bytes.set(src, this._length);
    this._length += src.length;
  }

  writeCString(value: string) {
    const encoded = textEncoder.encode(value);
    this.writeBytes(encoded);
    this.writeUint8(0);
  }

  writeString(value: string) {
    const encoded = textEncoder.encode(value);
    this.writeInt32(encoded.length + 1);
    this.writeBytes(encoded);
    this.writeUint8(0);
  }

  writeInt32(value: number) {
    this.ensureCapacity(4);
    this.view.setInt32(this._length, value, true);
    this._length += 4;
  }

  writeUint32(value: number) {
    this.ensureCapacity(4);
    this.view.setUint32(this._length, value, true);
    this._length += 4;
  }

  writeBigInt64(value: bigint) {
    this.ensureCapacity(8);
    if (typeof this.view.setBigInt64 === "function") {
      this.view.setBigInt64(this._length, value, true);
    } else {
      const low = Number(value & 0xffffffffn);
      const high = Number((value >> 32n) & 0xffffffffn);
      this.view.setUint32(this._length, low, true);
      this.view.setInt32(this._length + 4, high, true);
    }
    this._length += 8;
  }

  writeFloat64(value: number) {
    this.ensureCapacity(8);
    this.view.setFloat64(this._length, value, true);
    this._length += 8;
  }

  beginContainer(): number {
    const start = this._length;
    this.ensureCapacity(4);
    this.bytes.fill(0, start, start + 4);
    this._length += 4;
    return start;
  }

  endContainer(start: number) {
    this.writeUint8(0);
    const size = this._length - start;
    this.setInt32(start, size);
  }

  setInt32(offset: number, value: number) {
    this.view.setInt32(offset, value, true);
  }

  toUint8Array(): Uint8Array {
    return this.bytes.slice(0, this._length);
  }
}

function isPlainObject(x: any): boolean {
  return (
    x !== null &&
    typeof x === "object" &&
    !Array.isArray(x) &&
    !(x instanceof Uint8Array) &&
    !(x instanceof Date) &&
    !(x instanceof RegExp) &&
    !(x instanceof ObjectId)
  );
}

function encodeElement(writer: ByteWriter, key: string, value: any) {
  if (value instanceof ObjectId) {
    writer.writeUint8(Type.ObjectId);
    writer.writeCString(key);
    writer.writeBytes(value.id);
    return;
  }

  if (typeof value === "number") {
    if (
      Number.isInteger(value) &&
      value <= 0x7fffffff &&
      value >= -0x80000000
    ) {
      writer.writeUint8(Type.Int32);
      writer.writeCString(key);
      writer.writeInt32(value | 0);
    } else {
      writer.writeUint8(Type.Double);
      writer.writeCString(key);
      writer.writeFloat64(value);
    }
  } else if (typeof value === "bigint") {
    writer.writeUint8(Type.Int64);
    writer.writeCString(key);
    writer.writeBigInt64(value);
  } else if (typeof value === "string") {
    writer.writeUint8(Type.String);
    writer.writeCString(key);
    writer.writeString(value);
  } else if (value === null || value === undefined) {
    writer.writeUint8(Type.Null);
    writer.writeCString(key);
  } else if (typeof value === "boolean") {
    writer.writeUint8(Type.Boolean);
    writer.writeCString(key);
    writer.writeUint8(value ? 1 : 0);
  } else if (value instanceof Date) {
    writer.writeUint8(Type.DateTime);
    writer.writeCString(key);
    // milliseconds since epoch as int64
    writer.writeBigInt64(BigInt(value.getTime()));
  } else if (
    value instanceof Uint8Array ||
    (typeof Buffer !== "undefined" && value instanceof (Buffer as any))
  ) {
    const source =
      value instanceof Uint8Array
        ? value
        : new Uint8Array(
            (value as any).buffer,
            (value as any).byteOffset,
            (value as any).byteLength
          );
    writer.writeUint8(Type.Binary);
    writer.writeCString(key);
    writer.writeUint32(source.length);
    writer.writeUint8(0x00); // generic subtype
    writer.writeBytes(source);
  } else if (value instanceof RegExp) {
    writer.writeUint8(Type.Regex);
    writer.writeCString(key);
    writer.writeCString(value.source);
    // flags - only 'i','m','s','u','y' supported; convert to options string with mongo ordering (ignore ordering)
    writer.writeCString(value.flags);
  } else if (Array.isArray(value)) {
    writer.writeUint8(Type.Array);
    writer.writeCString(key);
    writeArray(writer, value);
  } else if (isPlainObject(value)) {
    writer.writeUint8(Type.Document);
    writer.writeCString(key);
    writeDocument(writer, value);
  } else {
    // fallback: try to serialize via toJSON or string
    if (value && typeof value.toJSON === "function") {
      encodeElement(writer, key, value.toJSON());
    } else {
      // as string
      writer.writeUint8(Type.String);
      writer.writeCString(key);
      writer.writeString(String(value));
    }
  }
}

function writeDocument(writer: ByteWriter, obj: any) {
  const start = writer.beginContainer();
  for (const k of Object.keys(obj)) {
    encodeElement(writer, k, obj[k]);
  }
  writer.endContainer(start);
}

function writeArray(writer: ByteWriter, arr: any[]) {
  const start = writer.beginContainer();
  for (let i = 0; i < arr.length; i++) {
    encodeElement(writer, String(i), arr[i]);
  }
  writer.endContainer(start);
}

export function serialize(obj: any): Uint8Array {
  const writer = new ByteWriter();
  writeDocument(writer, obj);
  return writer.toUint8Array();
}

function readCString(
  buf: Uint8Array,
  offset: number
): { str: string; next: number } {
  let end = offset;
  while (end < buf.length && buf[end] !== 0) end++;
  const s = textDecoder.decode(buf.subarray(offset, end));
  return { str: s, next: end + 1 };
}

function readInt32(buf: Uint8Array, offset: number): number {
  const dv = new DataView(buf.buffer, buf.byteOffset + offset, 4);
  return dv.getInt32(0, true);
}

function readUint32(buf: Uint8Array, offset: number): number {
  const dv = new DataView(buf.buffer, buf.byteOffset + offset, 4);
  return dv.getUint32(0, true);
}

function readInt64AsBigInt(buf: Uint8Array, offset: number): bigint {
  const dv = new DataView(buf.buffer, buf.byteOffset + offset, 8);
  // use getBigInt64 if available
  if (typeof dv.getBigInt64 === "function") {
    return dv.getBigInt64(0, true);
  }
  // fallback manual
  let low = BigInt(dv.getUint32(0, true));
  let high = BigInt(dv.getInt32(4, true));
  return (high << 32n) | low;
}

function readFloat64(buf: Uint8Array, offset: number): number {
  const dv = new DataView(buf.buffer, buf.byteOffset + offset, 8);
  return dv.getFloat64(0, true);
}

function decodeDocument(
  buf: Uint8Array,
  offset = 0
): { doc: any; next: number } {
  const size = readInt32(buf, offset);
  const end = offset + size;
  let pos = offset + 4;
  const out: any = {};
  while (pos < end - 1) {
    const type = buf[pos++];
    const { str: key, next } = readCString(buf, pos);
    pos = next;
    switch (type) {
      case Type.Double: {
        const val = readFloat64(buf, pos);
        pos += 8;
        out[key] = val;
        break;
      }
      case Type.String: {
        const slen = readInt32(buf, pos);
        pos += 4;
        const s = textDecoder.decode(buf.subarray(pos, pos + slen - 1));
        pos += slen;
        out[key] = s;
        break;
      }
      case Type.Document: {
        const sub = decodeDocument(buf, pos);
        out[key] = sub.doc;
        pos = sub.next;
        break;
      }
      case Type.Array: {
        const sub = decodeDocument(buf, pos);
        // convert numeric keys to array
        const arr: any[] = [];
        for (const k of Object.keys(sub.doc)) {
          arr[Number(k)] = sub.doc[k];
        }
        out[key] = arr;
        pos = sub.next;
        break;
      }
      case Type.Binary: {
        const blen = readUint32(buf, pos);
        pos += 4;
        const subtype = buf[pos++];
        const data = buf.subarray(pos, pos + blen);
        pos += blen;
        out[key] = new Uint8Array(data);
        break;
      }
      case Type.ObjectId: {
        const id = buf.subarray(pos, pos + 12);
        pos += 12;
        out[key] = new ObjectId(new Uint8Array(id));
        break;
      }
      case Type.Boolean: {
        const b = buf[pos++] !== 0;
        out[key] = b;
        break;
      }
      case Type.DateTime: {
        const ms = readInt64AsBigInt(buf, pos);
        pos += 8;
        out[key] = new Date(Number(ms));
        break;
      }
      case Type.Null: {
        out[key] = null;
        break;
      }
      case Type.Regex: {
        const { str: pattern, next: n1 } = readCString(buf, pos);
        const { str: options, next: n2 } = readCString(buf, n1);
        pos = n2;
        let flags = options;
        // construct RegExp; try/catch in case of invalid pattern
        try {
          out[key] = new RegExp(pattern, flags);
        } catch {
          out[key] = { pattern, options };
        }
        break;
      }
      case Type.Int32: {
        const v = readInt32(buf, pos);
        pos += 4;
        out[key] = v;
        break;
      }
      case Type.Int64: {
        const v = readInt64AsBigInt(buf, pos);
        pos += 8;
        out[key] = v; // bigint
        break;
      }
      default: {
        // unknown type: skip remaining document to avoid infinite loop
        throw new Error("Unsupported BSON type: 0x" + type!.toString(16));
      }
    }
  }
  // terminator
  pos++; // skip 0
  return { doc: out, next: end };
}

export function deserialize(buf: ArrayBuffer | Uint8Array | DataView): any {
  let u8: Uint8Array;
  if (buf instanceof Uint8Array) {
    u8 = buf;
  } else if (buf instanceof DataView) {
    u8 = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  } else {
    u8 = new Uint8Array(buf);
  }
  const { doc } = decodeDocument(u8, 0);
  return doc;
}
