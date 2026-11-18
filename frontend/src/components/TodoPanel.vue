<template>
  <section class="lifecycle-panel" data-testid="todo-panel">
    <header class="panel-head">
      <div>
        <h3>关联待办</h3>
        <p class="page-desc">共享键等待释放、检查点占用、发布阻断会在相关页面生成待办。</p>
      </div>
      <button class="btn" type="button" data-testid="refresh-todos" @click="reload">刷新</button>
    </header>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table" data-testid="todo-table">
      <thead>
        <tr><th>页面</th><th>状态键</th><th>说明</th><th>状态</th><th>操作</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in todos" :key="row.id" :data-testid="`todo-${row.id}`" :data-status="row.status">
          <td>{{ row.module }}</td>
          <td>{{ row.state_key || '—' }}</td>
          <td>{{ row.message }}</td>
          <td>{{ row.status }}</td>
          <td>
            <button
              v-if="row.status === '待处理'"
              class="link"
              type="button"
              :data-testid="`resolve-${row.id}`"
              @click="resolve(row.id)"
            >标记已处理</button>
            <span v-else class="muted">已处理</span>
          </td>
        </tr>
        <tr v-if="!todos.length">
          <td colspan="5" class="empty-state">暂无待办</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { loadTodos, resolveTodo, type RestoreTodo } from '@/api/state-lifecycle'

const props = defineProps<{ module?: string }>()

const todos = ref<RestoreTodo[]>([])
const errorMessage = ref('')

async function reload() {
  errorMessage.value = ''
  try {
    todos.value = await loadTodos(props.module)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '待办读取失败'
  }
}

async function resolve(id: number) {
  errorMessage.value = ''
  try {
    await resolveTodo(id)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '待办处理失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.lifecycle-panel { margin-top: 20px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.panel-head h3 { margin: 0; font-size: 15px; }
.muted { color: var(--muted); }
</style>
