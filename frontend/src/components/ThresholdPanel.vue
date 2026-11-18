<template>
  <section class="lifecycle-panel" data-testid="threshold-panel">
    <header class="panel-head">
      <div>
        <h3>阈值核对</h3>
        <p class="page-desc">
          按当前口径的判定条件核对状态恢复数据，保留全部仍有引用的分组。
        </p>
      </div>
      <button class="btn" type="button" data-testid="refresh-threshold" @click="reload">核对</button>
    </header>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>状态配置</span>
        <select v-model="config" data-testid="threshold-config">
          <option v-for="item in configs" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>清理口径</span>
        <select v-model="mode" data-testid="threshold-mode">
          <option value="deferred">统一清理</option>
          <option value="refcount">引用计数</option>
        </select>
      </label>
      <label class="filter-item">
        <span>最少分组</span>
        <input v-model.number="minGroups" type="number" min="0" data-testid="threshold-min-groups" />
      </label>
      <button class="btn primary" data-testid="threshold-run" type="submit">执行核对</button>
    </form>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <div v-if="result" class="analysis-summary" data-testid="threshold-summary">
      <article class="analysis-card">
        <span class="stat-label">判定结果</span>
        <strong :class="result.passed ? 'trend-up' : 'trend-down'" data-testid="threshold-verdict">
          {{ result.passed ? '通过' : '未通过' }}
        </strong>
      </article>
      <article class="analysis-card">
        <span class="stat-label">应有分组</span>
        <strong data-testid="threshold-expected">{{ result.receipt.expected_groups }}</strong>
      </article>
      <article class="analysis-card">
        <span class="stat-label">保留分组</span>
        <strong data-testid="threshold-retained">{{ result.receipt.retained_groups }}</strong>
      </article>
      <article class="analysis-card">
        <span class="stat-label">缺失分组</span>
        <strong class="trend-down" data-testid="threshold-missing">
          {{ result.receipt.missing_groups.length }}
        </strong>
      </article>
    </div>

    <p v-if="result" class="condition-range" data-testid="threshold-range">
      当前口径：{{ result.mode === 'deferred' ? '等待最后一个引用释放' : '引用计数逐次递减' }}
      ｜范围：{{ result.condition_range.scope || '全部状态键' }}
      ｜最少分组：{{ result.condition_range.min_groups }}
    </p>

    <table v-if="result" class="data-table" data-testid="threshold-table">
      <thead>
        <tr><th>状态键</th><th>应有分组</th><th>保留分组</th><th>缺失</th><th>来源版本</th><th>规则命中</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in result.keys" :key="row.state_key" :data-status="row.passed ? '通过' : '未通过'">
          <td>{{ row.state_key }}</td>
          <td>{{ row.expected_groups }}</td>
          <td>{{ row.retained_groups }}</td>
          <td>
            <span v-for="group in row.missing_groups" :key="`${group.operator}-${group.group}`" class="tag missing">
              {{ group.operator }}/{{ group.group }}
            </span>
            <span v-if="!row.missing_groups.length" class="muted">无</span>
          </td>
          <td>{{ row.source_version || '—' }}</td>
          <td>{{ row.rule_hits.map((hit) => hit.rule).join('、') || '—' }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { listEntries } from '@/api/backend-service'
import { loadThreshold, type ThresholdCheck } from '@/api/state-lifecycle'

const configs = ref<string[]>([])
const config = ref('')
const mode = ref<'deferred' | 'refcount'>('deferred')
const minGroups = ref(0)
const result = ref<ThresholdCheck | null>(null)
const errorMessage = ref('')

async function loadConfigs() {
  try {
    const payload = await listEntries('state_store', {})
    configs.value = payload.items
      .map((row) => String(row['配置名称'] ?? ''))
      .filter((name) => name.length > 0)
    if (!config.value) config.value = configs.value[0] ?? ''
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '状态配置读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  if (!config.value) {
    errorMessage.value = '请先选择状态配置'
    return
  }
  try {
    result.value = await loadThreshold(config.value, mode.value, minGroups.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '阈值核对失败'
  }
}

onMounted(async () => {
  await loadConfigs()
  if (config.value) await reload()
})
</script>

<style scoped>
.lifecycle-panel { margin-top: 20px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.panel-head h3 { margin: 0; font-size: 15px; }
.condition-range { color: var(--muted); font-size: 12px; }
.tag.missing { background: #fee2e2; color: #991b1b; }
.tag { display: inline-block; padding: 1px 6px; margin-right: 4px; border-radius: 4px; font-size: 12px; }
.muted { color: var(--muted); }
select, input { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
</style>
