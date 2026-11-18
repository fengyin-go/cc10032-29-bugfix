<template>
  <section class="lifecycle-panel" data-testid="checkpoint-panel">
    <header class="panel-head">
      <div>
        <h3>检查点状态压缩清单</h3>
        <p class="page-desc">
          按状态压缩清单和活动引用验证恢复数据；缺键的检查点不允许发布回放结果。
        </p>
      </div>
      <button class="btn" type="button" data-testid="refresh-checkpoint" @click="reload">刷新</button>
    </header>

    <form class="filter-bar" @submit.prevent="inspect">
      <label class="filter-item">
        <span>检查点</span>
        <select v-model="checkpointId" data-testid="checkpoint-select">
          <option v-for="item in checkpoints" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <button class="btn primary" data-testid="checkpoint-inspect" type="submit">查看清单</button>
    </form>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <div v-if="manifest" data-testid="checkpoint-manifest">
      <p class="condition-range">
        来源版本：{{ manifest.source_version || '—' }}
        ｜验证：{{ validationPassed ? '通过' : '缺键/不一致' }}
      </p>
      <table class="data-table">
        <thead>
          <tr><th>状态键</th><th>压缩清单</th><th>保留</th><th>已压缩</th><th>引用算子</th><th>检查点占用</th></tr>
        </thead>
        <tbody>
          <tr v-for="entry in manifest.entries" :key="entry.state_key">
            <td>{{ entry.state_key }}</td>
            <td>{{ entry.manifest_id || '—' }}</td>
            <td>{{ entry.kept_ids.join('、') || '—' }}</td>
            <td>{{ entry.compacted_ids.join('、') || '—' }}</td>
            <td>{{ entry.operators.join('、') || '—' }}</td>
            <td>{{ entry.pinned ? '是' : '否' }}</td>
          </tr>
          <tr v-if="!manifest.entries.length">
            <td colspan="6" class="empty-state">该检查点没有引用状态键</td>
          </tr>
        </tbody>
      </table>
      <div v-if="ruleHits.length" class="plan-block" data-testid="checkpoint-rules">
        <h4>规则命中</h4>
        <span v-for="hit in ruleHits" :key="hit.rule" class="tag rule">{{ hit.rule }}</span>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { listEntries } from '@/api/backend-service'
import { loadCheckpointManifest, type CheckpointManifest } from '@/api/state-lifecycle'

const checkpoints = ref<string[]>([])
const checkpointId = ref('')
const manifest = ref<CheckpointManifest | null>(null)
const errorMessage = ref('')

const validationPassed = computed(() => {
  const validation = manifest.value?.restore_validation as
    | { valid?: boolean }
    | undefined
  return validation?.valid !== false
})

const ruleHits = computed(() => {
  const validation = manifest.value?.restore_validation as
    | { rule_hits?: { rule: string; hit: boolean }[] }
    | undefined
  return (validation?.rule_hits ?? []).filter((hit) => hit.hit)
})

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries('checkpoint_mgr', {})
    checkpoints.value = payload.items
      .map((row) => String(row.checkpoint_id ?? row.id ?? ''))
      .filter((id) => id.length > 0)
    if (!checkpointId.value) checkpointId.value = checkpoints.value[0] ?? ''
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '检查点读取失败'
  }
}

async function inspect() {
  errorMessage.value = ''
  if (!checkpointId.value) {
    errorMessage.value = '请先选择检查点'
    return
  }
  try {
    manifest.value = await loadCheckpointManifest(checkpointId.value)
  } catch (error) {
    manifest.value = null
    errorMessage.value = error instanceof Error ? error.message : '压缩清单读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.lifecycle-panel { margin-top: 20px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.panel-head h3 { margin: 0; font-size: 15px; }
.condition-range { color: var(--muted); font-size: 12px; }
.plan-block { margin-top: 10px; }
.plan-block h4 { margin: 8px 0; font-size: 14px; }
.tag.rule { background: #fef3c7; color: #92400e; }
.tag { display: inline-block; padding: 1px 6px; margin-right: 4px; border-radius: 4px; font-size: 12px; }
select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
</style>
