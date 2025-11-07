<script lang="ts">
  import { Bucket, ObjectId, compileWasmModule } from "albedo-wasm";
  import wasm from "albedo-wasm/albedo.wasm?url";
	import { onMount } from "svelte";
  let tasksBucket: Bucket;
  let tasks = $state<Array<{ _id: string, task: string; completed: boolean; inserted_at: Date }>>([]);
  
  onMount(async () => {
    await compileWasmModule(wasm);
    tasksBucket = Bucket.open("tasks.bucket");
    reloadTasks();
  });

  function reloadTasks() {
    tasks = tasksBucket.all({}, {sort: { desc: 'inserted_at' } });
    console.log(tasks)
  }

  let taskInput = $state('');
  function handleSubmit(event: Event) {
    event.preventDefault();
    if (taskInput) {
      // Add logic to handle the new task (e.g., save to database or state)
      console.log('New Task:', taskInput);
      tasksBucket.insert({ 
        task: taskInput, completed: false, 
        inserted_at: new Date(), _id: new ObjectId().toString(), 
      });
      taskInput = '';
    }
    reloadTasks();
  }

  function deleteTask(id: string) {
    tasksBucket.delete({ _id: id });
    reloadTasks();
  }

  function markCompleted(id: string, completed: boolean) {
    tasksBucket.transform({ _id: id }, task => ({ ...task, completed }));
      reloadTasks();
  }

</script>

<main class="mx-auto w-3xl p-10">
  <h1 class="text-3xl font-bold underline">Tasks!</h1>
  <form onsubmit={handleSubmit}>
    <input
      type="text"
      bind:value={taskInput}
      placeholder="New task"
      class="border border-gray-300 rounded-md p-2 w-full mt-4"
    />
  </form>
  <div>
    {#each tasks as task (task.inserted_at)}
      <div class="border-b border-gray-200 flex flex-row py-2">
        <input onchange={(e) => markCompleted(task._id,e.currentTarget.checked )} type="checkbox" bind:checked={task.completed} class="mr-2" />
        <span class="{task.completed ? 'line-through text-gray-500' : ''} grow">{task.task}</span>
        <button onclick={() => deleteTask(task._id)} class="ml-4 text-red-500 hover:text-red-700">
          {@render deleteIcon()}
        </button>
      </div>
    {/each}
  </div>
</main>

{#snippet deleteIcon()}
  <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24"><!-- Icon from Material Symbols by Google - https://github.com/google/material-design-icons/blob/master/LICENSE --><path fill="currentColor" d="M7 21q-.825 0-1.412-.587T5 19V6q-.425 0-.712-.288T4 5t.288-.712T5 4h4q0-.425.288-.712T10 3h4q.425 0 .713.288T15 4h4q.425 0 .713.288T20 5t-.288.713T19 6v13q0 .825-.587 1.413T17 21zM17 6H7v13h10zm-7 11q.425 0 .713-.288T11 16V9q0-.425-.288-.712T10 8t-.712.288T9 9v7q0 .425.288.713T10 17m4 0q.425 0 .713-.288T15 16V9q0-.425-.288-.712T14 8t-.712.288T13 9v7q0 .425.288.713T14 17M7 6v13z"/></svg>
{/snippet}