<template>
  <section class="lifecycle-panel" data-testid="restore-panel">
    <header class="panel-head">
      <div>
        <h3>状态恢复数据</h3>
        <p class="page-desc">
          两种清理口径各自形成版本，但每个状态键只保留一个可消费的活动版本，并记录来源版本。
        </p>
      </div>
      <button class="btn" type="button" data-testid="refresh-restore" @click="reload">刷新</button>
    </header>

    <form class="filter-bar" @submit.prevent="check">
      <label class="filter-item">
        <span>恢复范围</span>
        <input v-model="scopeInput" data-testid="restore-scope" placeholder="按状态键或配置名称核对" />
      </label>
      <button class="btn" data-testid="restore-plan-check" type="submit">核对恢复数据</button>
    </form>

    <p v-if="planCheck" :class="planCheck.all_valid ? 'hint-text' : 'error-text'" data-testid="restore-plan-summary">
      {{ planCheck.summary }}
      <span v-for="entry in planCheck.entries.filter((item) => !item.valid)" :key="entry.state_key">
        ｜{{ entry.state_key }} 缺少 {{ entry.missing_groups.map((group) => `${group.operator}/${group.group}`).join('、') }}
      </span>
    </p>
    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table" data-testid="restore-table">
      <thead>
        <tr>
          <th>状态键</th>
          <th>口径</th>
          <th>版本</th>
          <th>状态</th>
          <th>来源版本</th>
          <th>分组</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in versions"
          :key="row.restore_id"
          :data-testid="`restore-${row.restore_id}`"
          :data-status="row.status"
        >
          <td>{{ row.state_key }}</td>
          <td>{{ row.mode === 'deferred' ? '统一清理' : '引用计数' }}</td>
          <td>v{{ row.version }}</td>
          <td>{{ row.status }}</td>
          <td>{{ row.source_version || '—' }}</td>
          <td>
            <span
              v-for="group in row.groups"
              :key="`${group.operator}-${group.group}`"
              class="tag"
            >{{ group.operator }}/{{ group.group }}</span>
            <span v-if="!row.groups.length" class="muted">无可消费分组</span>
          </td>
        </tr>
        <tr v-if="!versions.length">
          <td colspan="6" class="empty-state">还没有生成状态恢复数据</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  loadRestoreData,
  loadRestorePlanCheck,
  type RestorePlanCheck,
  type RestoreVersion,
} from '@/api/state-lifecycle'

const props = defineProps<{ scope?: string }>()

const versions = ref<RestoreVersion[]>([])
const errorMessage = ref('')
const scopeInput = ref(props.scope ?? '')
const planCheck = ref<RestorePlanCheck | null>(null)

async function reload() {
  errorMessage.value = ''
  try {
    const params: Record<string, string> = {}
    if (props.scope) params.state_key = props.scope
    const payload = await loadRestoreData(params)
    versions.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '状态恢复数据读取失败'
  }
}

async function check() {
  errorMessage.value = ''
  try {
    planCheck.value = await loadRestorePlanCheck(scopeInput.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复数据核对失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.lifecycle-panel { margin-top: 20px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.panel-head h3 { margin: 0; font-size: 15px; }
.tag { display: inline-block; padding: 1px 6px; margin: 1px 4px 1px 0; border-radius: 4px; background: #eef2ff; color: #1f3a8a; font-size: 12px; }
.muted { color: var(--muted); }
.hint-text { color: #166534; font-size: 13px; }
input { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
</style>
