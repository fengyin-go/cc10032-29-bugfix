<template>
  <section class="lifecycle-panel" data-testid="replay-verify-panel">
    <header class="panel-head">
      <div>
        <h3>回放发布校验</h3>
        <p class="page-desc">
          发布前按状态压缩清单和活动引用复核，共享键被提前移除时阻断发布并生成待办。
        </p>
      </div>
      <button class="btn" type="button" data-testid="refresh-replay" @click="reload">刷新</button>
    </header>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table" data-testid="replay-table">
      <thead>
        <tr><th>任务</th><th>源检查点</th><th>状态</th><th>来源版本</th><th>发布校验</th><th>操作</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`replay-row-${row.id}`">
          <td>{{ row['任务名称'] }}</td>
          <td>{{ row['源检查点'] }}</td>
          <td>{{ row.status }}</td>
          <td>{{ row.source_version || '—' }}</td>
          <td>
            <span v-if="checks[Number(row.id)]" :data-testid="`publish-${row.id}`">
              {{ checks[Number(row.id)] ? (checkPassed(Number(row.id)) ? '可发布' : '缺键阻断') : '待校验' }}
            </span>
            <span v-else class="muted">待校验</span>
          </td>
          <td>
            <button class="link" type="button" :data-testid="`check-${row.id}`" @click="verify(row)">
              校验
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td colspan="6" class="empty-state">暂无回放任务</td>
        </tr>
      </tbody>
    </table>

    <div v-if="blocked.length" class="plan-block" data-testid="blocked-groups">
      <h4>阻断原因</h4>
      <p v-for="item in blocked" :key="item.taskId" class="error-text">
        任务 {{ item.taskId }} 缺少分组：
        <span v-for="group in item.groups" :key="`${group.operator}-${group.group}`" class="tag missing">
          {{ group.operator }}/{{ group.group }}
        </span>
        <span v-if="!item.groups.length">状态键 {{ item.keys.join('、') }}</span>
      </p>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { listEntries } from '@/api/backend-service'
import { publishCheck, type PublishCheck } from '@/api/state-lifecycle'
import type { EntryRow } from '@/data/types'

const rows = ref<EntryRow[]>([])
const checks = ref<Record<number, PublishCheck>>({})
const blocked = ref<
  { taskId: number; groups: { operator: string; group: string }[]; keys: string[] }[]
>([])
const errorMessage = ref('')

function checkPassed(taskId: number): boolean {
  return Boolean(checks.value[taskId]?.publishable)
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries('replay_engine', {})
    rows.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回放任务读取失败'
  }
}

async function verify(row: EntryRow) {
  errorMessage.value = ''
  const taskId = Number(row.id)
  try {
    const result = await publishCheck(taskId)
    checks.value = { ...checks.value, [taskId]: result }
    blocked.value = blocked.value.filter((item) => item.taskId !== taskId)
    if (!result.publishable) {
      blocked.value.push({
        taskId,
        groups: result.restore_check?.missing_groups ?? [],
        keys: result.restore_check?.missing_keys ?? [],
      })
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '发布校验失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.lifecycle-panel { margin-top: 20px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.panel-head h3 { margin: 0; font-size: 15px; }
.plan-block { margin-top: 12px; }
.plan-block h4 { margin: 8px 0; font-size: 14px; }
.tag.missing { background: #fee2e2; color: #991b1b; }
.tag { display: inline-block; padding: 1px 6px; margin-right: 4px; border-radius: 4px; font-size: 12px; }
.muted { color: var(--muted); }
</style>
