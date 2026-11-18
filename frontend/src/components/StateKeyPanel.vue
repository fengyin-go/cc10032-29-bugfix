<template>
  <section class="lifecycle-panel" data-testid="state-key-panel">
    <header class="panel-head">
      <div>
        <h3>状态键引用与TTL清理</h3>
        <p class="page-desc">
          清理前先读取状态键、判断全部算子引用；只有引用都过期且没有检查点占用时才停用。
        </p>
      </div>
      <button class="btn" type="button" data-testid="refresh-keys" @click="reload">刷新</button>
    </header>

    <form class="filter-bar" data-testid="cleanup-form" @submit.prevent="submitCleanup">
      <label class="filter-item">
        <span>状态配置</span>
        <select v-model="config" data-testid="cleanup-config">
          <option v-for="item in configs" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>清理口径</span>
        <select v-model="mode" data-testid="cleanup-mode">
          <option value="deferred">等待最后一个引用释放后统一清理</option>
          <option value="refcount">按引用计数逐次递减</option>
        </select>
      </label>
      <button class="btn" type="button" data-testid="cleanup-preview" @click="preview">预演</button>
      <button class="btn primary" data-testid="cleanup-run" type="submit" :disabled="busy">
        {{ busy ? '提交中…' : '执行清理' }}
      </button>
    </form>

    <p v-if="message" class="hint-text" data-testid="cleanup-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table" data-testid="state-key-table">
      <thead>
        <tr>
          <th>状态键</th>
          <th>引用算子</th>
          <th>分组</th>
          <th>检查点引用</th>
          <th>引用状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="entry in keys" :key="entry.state_key" :data-testid="`state-key-${entry.state_key}`">
          <td>{{ entry.state_key }}</td>
          <td>
            <span v-for="operator in entry.operators" :key="operator" class="tag">{{ operator }}</span>
            <span v-if="!entry.operators.length" class="muted">无活动引用</span>
          </td>
          <td>{{ entry.groups.join('、') || '—' }}</td>
          <td>
            <span v-if="entry.pinned" class="tag pinned">{{ entry.checkpoint_ids.join('、') }}</span>
            <span v-else class="muted">未被引用</span>
          </td>
          <td>{{ entry.active_rows }} 条状态 / {{ entry.references.length }} 个引用</td>
        </tr>
        <tr v-if="!keys.length">
          <td colspan="5" class="empty-state">暂无状态键</td>
        </tr>
      </tbody>
    </table>

    <div v-if="planRows.length" class="plan-block" data-testid="cleanup-plan">
      <h4>清理预演（{{ mode === 'deferred' ? '统一清理' : '引用计数' }}）</h4>
      <table class="data-table">
        <thead>
          <tr><th>状态键</th><th>动作</th><th>活动算子</th><th>过期算子</th></tr>
        </thead>
        <tbody>
          <tr v-for="row in planRows" :key="row.state_key">
            <td>{{ row.state_key }}</td>
            <td>{{ actionLabel(row.action) }}</td>
            <td>{{ row.live_operators.join('、') || '—' }}</td>
            <td>{{ row.expired_operators.join('、') || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="receipts.length" class="plan-block" data-testid="cleanup-receipts">
      <h4>批量回执</h4>
      <table class="data-table">
        <thead>
          <tr><th>回执</th><th>口径</th><th>清理条数</th><th>保留键</th><th>版本</th></tr>
        </thead>
        <tbody>
          <tr v-for="row in receipts" :key="row.receipt_id">
            <td>{{ row.receipt_id }}</td>
            <td>{{ row.mode === 'deferred' ? '统一清理' : '引用计数' }}</td>
            <td>{{ row.cleaned_state_ids.length }}</td>
            <td>{{ row.retained_keys.join('、') || '—' }}</td>
            <td>{{ row.version_ids.length }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { listEntries } from '@/api/backend-service'
import {
  loadCleanupPlan,
  loadReceipts,
  loadStateKeys,
  runCleanup,
  type CleanupDecision,
  type CleanupReceipt,
  type StateKeyEntry,
} from '@/api/state-lifecycle'

const keys = ref<StateKeyEntry[]>([])
const configs = ref<string[]>([])
const config = ref('')
const mode = ref<'deferred' | 'refcount'>('deferred')
const planRows = ref<CleanupDecision[]>([])
const receipts = ref<CleanupReceipt[]>([])
const message = ref('')
const errorMessage = ref('')
const busy = ref(false)

const ACTION_LABELS: Record<string, string> = {
  cleanup: '停用状态键',
  refcount: '递减过期引用',
  wait_last_release: '等待最后一个引用释放',
  hold_checkpoint: '检查点占用，保留',
  active: '仍有活动引用，保留',
  noop: '无需处理',
}

function actionLabel(action: string): string {
  return ACTION_LABELS[action] ?? action
}

async function reload() {
  errorMessage.value = ''
  try {
    const [index, configPayload] = await Promise.all([
      loadStateKeys(),
      listEntries('state_store', {}),
    ])
    keys.value = index.keys
    configs.value = configPayload.items
      .map((row) => String(row['配置名称'] ?? ''))
      .filter((name) => name.length > 0)
    if (!config.value) config.value = configs.value[0] ?? ''
    if (config.value) receipts.value = await loadReceipts(config.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '状态键读取失败'
  }
}

async function preview() {
  errorMessage.value = ''
  if (!config.value) return
  try {
    const payload = await loadCleanupPlan(config.value, mode.value)
    planRows.value = payload.keys
    receipts.value = await loadReceipts(config.value)
    message.value = '预演完成，未写入任何状态'
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '清理预演失败'
  }
}

async function submitCleanup() {
  errorMessage.value = ''
  if (!config.value) {
    errorMessage.value = '请先选择状态配置'
    return
  }
  busy.value = true
  try {
    const requestId = `${config.value}-${mode.value}-${Date.now()}`
    const result = await runCleanup(config.value, mode.value, requestId)
    if (!result.ok) throw new Error(result.error ?? '清理失败')
    message.value = result.duplicate
      ? `同一提交已落过回执：${result.receipt_id}`
      : `回执 ${result.receipt_id}：清理 ${result.cleaned ?? 0} 条，保留 ${(result.retained_keys ?? []).length} 个键`
    planRows.value = result.decisions ?? []
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '清理失败'
  } finally {
    busy.value = false
  }
}

onMounted(reload)
</script>

<style scoped>
.lifecycle-panel { margin-top: 20px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.panel-head h3 { margin: 0; font-size: 15px; }
.tag { display: inline-block; padding: 1px 6px; margin-right: 4px; border-radius: 4px; background: #eef2ff; color: #1f3a8a; font-size: 12px; }
.tag.pinned { background: #fef3c7; color: #92400e; }
.muted { color: var(--muted); }
.hint-text { color: #166534; font-size: 13px; }
.plan-block { margin-top: 14px; }
.plan-block h4 { margin: 8px 0; font-size: 14px; }
select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
</style>
