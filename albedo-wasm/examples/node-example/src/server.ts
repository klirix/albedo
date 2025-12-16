import { Bucket, ObjectId } from "albedo-wasm";
import { env } from "bun";
import { Elysia, t } from "elysia";

export const bucket = Bucket.open(env.DB_FILE || "example-bucket.bucket");

const todoSchema = t.Object({
  _id: t.String(),
  title: t.String(),
  completed: t.Boolean(),
  createdAt: t.String(),
});

const errorSchema = t.Object({
  error: t.String(),
});

export const server = new Elysia();

server.get(
  "/todos",
  () => {
    return bucket.list();
  },
  { response: t.Array(todoSchema) }
);

server.post(
  "/todos",
  async ({ body }) => {
    const _id = new ObjectId();
    const newTodo = {
      _id: _id,
      title: body.title,
      completed: false,
      createdAt: new Date(),
    };
    bucket.insert(newTodo);
    return {
      ...newTodo,
      _id: _id.toString(),
      createdAt: newTodo.createdAt.toISOString(),
    };
  },
  {
    body: t.Object({ title: t.String() }),
    response: todoSchema,
  }
);

server.put(
  "/todos/:id",
  async ({ params, body, status }) => {
    const id = ObjectId.fromString(params.id);
    const todo = bucket.get({ _id: id });
    if (!todo) {
      return status(404, { error: "Todo not found" });
    }
    const updatedTodo = { ...todo, ...body };
    bucket.transform({ _id: id }, (_) => updatedTodo);
    return updatedTodo;
  },
  {
    params: t.Object({ id: t.String() }),
    body: t.Object({
      title: t.Optional(t.String()),
      completed: t.Optional(t.Boolean()),
    }),
    response: {
      200: todoSchema,
      404: errorSchema,
    },
  }
);

server.delete(
  "/todos/:id",
  async ({ params, status }) => {
    const id = ObjectId.fromString(params.id);
    const todo = bucket.get({ _id: id });
    if (!todo) {
      return status(404, { error: "Todo not found" });
    }
    bucket.delete({ _id: id });
    return { success: true };
  },
  {
    params: t.Object({ id: t.String() }),
    response: {
      200: t.Object({ success: t.Boolean() }),
      404: errorSchema,
    },
  }
);
