import { fetchJson, request } from './client'

export type StateReference = {
  state_id: number | null
  operator: string
  group: string
  last_active: number | null
  ttl: number | null
  released: boolean
  inactive: boolean
  deleted_at: number | null
  status: string
}

export type StateKeyEntry = {
  state_key: string
  operators: string[]
  all_operators: string[]
  groups: string[]
  checkpoint_ids: string[]
  pinned: boolean
  active_rows: number
  references: StateReference[]
}

export type StateKeyIndex = {
  ok: boolean
  key_count: number
  checkpoint_count: number
  keys: StateKeyEntry[]
}

export type RestoreGroup = {
  operator: string
  group: string
  state_id?: number | null
  aggregates?: Record<string, number>
  last_active?: number | null
  reference_state?: string
}

export type RestoreVersion = {
  restore_id: string
  state_key: string
  mode: string
  version: number
  status: string
  source_version: string | null
  source_manifest_id: string | null
  group_count: number
  groups: RestoreGroup[]
  operators: string[]
  checkpoint_ids: string[]
  retained: boolean
  receipt_id: string | null
  created_at: number
}

export type RestoreDataResult = {
  items: RestoreVersion[]
  total: number
  active_count: number
  active_version: string | null
}

export type CleanupDecision = {
  state_key: string
  action: string
  mode: string
  pinned: boolean
  checkpoint_ids: string[]
  live_operators: string[]
  expired_operators: string[]
  kept_state_ids?: (number | null)[]
  deleted_state_ids?: (number | null)[]
}

export type CleanupResult = {
  ok: boolean
  duplicate?: boolean
  receipt_id?: string
  config: string
  mode: string
  cleaned?: number
  cleaned_keys?: string[]
  retained_keys?: string[]
  held_keys?: string[]
  decisions?: CleanupDecision[]
  version_ids?: string[]
  error?: string
  rolled_back?: boolean
  message?: string
}

export type ThresholdCheck = {
  ok: boolean
  config: string
  mode: string
  passed: boolean
  condition_range: Record<string, unknown>
  receipt: {
    receipt_id: string
    expected_groups: number
    retained_groups: number
    missing_groups: { state_key?: string; operator: string; group: string }[]
    passed: boolean
    created_at: number
  }
  keys: {
    state_key: string
    expected_groups: number
    retained_groups: number
    missing_groups: { operator: string; group: string }[]
    restore_id: string | null
    source_version: string | null
    pinned: boolean
    passed: boolean
    rule_hits: { rule: string; hit: boolean }[]
  }[]
}

export type CleanupReceipt = {
  receipt_id: string
  config: string
  mode: string
  status: string
  cleaned_state_ids: (number | null)[]
  deleted_keys: string[]
  retained_keys: string[]
  held_keys: string[]
  version_ids: string[]
  created_at: number
}

export type RestoreTodo = {
  id: number
  kind: string
  module: string
  page: string
  config: string
  mode: string | null
  state_key: string | null
  message: string
  status: string
  created_at: number
}

export type CheckpointManifest = {
  ok: boolean
  checkpoint_id: string
  config: string
  state_key_manifest: string[]
  source_version?: string | null
  restore_validation?: Record<string, unknown>
  entries: {
    state_key: string
    manifest_id: string | null
    kept_ids: number[]
    compacted_ids: number[]
    groups: string[]
    operators: string[]
    pinned: boolean
  }[]
}

export type PublishCheck = {
  ok: boolean
  task_id: number
  restore_id: string | null
  source_version: string | null
  publishable: boolean
  restore_check?: {
    valid: boolean
    missing_keys: string[]
    missing_groups: { operator: string; group: string }[]
    source_version: string | null
  }
}

export async function loadStateKeys(): Promise<StateKeyIndex> {
  return await fetchJson<StateKeyIndex>('/api/state-lifecycle/keys')
}

export async function loadCleanupPlan(
  config: string,
  mode: string,
): Promise<{ keys: CleanupDecision[] }> {
  const query = new URLSearchParams({ mode }).toString()
  return await fetchJson(`/api/state-lifecycle/state-keys/${encodeURIComponent(config)}/plan?${query}`)
}

export async function runCleanup(
  config: string,
  mode: string,
  requestId: string,
): Promise<CleanupResult> {
  const query = new URLSearchParams({ mode, request_id: requestId }).toString()
  const response = await request(
    `/api/state-lifecycle/state-keys/${encodeURIComponent(config)}/cleanup?${query}`,
    { method: 'POST' },
  )
  return await response.json()
}

export async function loadReceipts(config: string): Promise<CleanupReceipt[]> {
  const payload = await fetchJson<{ items: CleanupReceipt[] }>(
    `/api/state-lifecycle/state-keys/${encodeURIComponent(config)}/receipts`,
  )
  return payload.items
}

export async function loadRestoreData(
  params: Record<string, string> = {},
): Promise<RestoreDataResult> {
  const query = new URLSearchParams(params).toString()
  return await fetchJson<RestoreDataResult>(`/api/state-lifecycle/restore-data?${query}`)
}

export async function loadThreshold(
  config: string,
  mode: string,
  minGroups = 0,
): Promise<ThresholdCheck> {
  const query = new URLSearchParams({
    mode,
    min_groups: String(minGroups),
  }).toString()
  return await fetchJson<ThresholdCheck>(
    `/api/state-lifecycle/state-keys/${encodeURIComponent(config)}/threshold?${query}`,
  )
}

export async function loadCheckpointManifest(
  checkpointId: string,
): Promise<CheckpointManifest> {
  return await fetchJson<CheckpointManifest>(
    `/api/state-lifecycle/checkpoints/${encodeURIComponent(checkpointId)}/manifest`,
  )
}

export async function loadTodos(module?: string): Promise<RestoreTodo[]> {
  const query = module ? `?${new URLSearchParams({ module }).toString()}` : ''
  const payload = await fetchJson<{ items: RestoreTodo[] }>(
    `/api/state-lifecycle/todos${query}`,
  )
  return payload.items
}

export async function resolveTodo(id: number): Promise<void> {
  const response = await request(`/api/state-lifecycle/todos/${id}/resolve`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('待办处理失败')
}

export type RestorePlanCheck = {
  ok: boolean
  scope: string
  all_valid: boolean
  summary: string
  entries: {
    state_key: string
    restore_id: string | null
    source_version: string | null
    expected_groups: number
    retained_groups: number
    missing_groups: { operator: string; group: string }[]
    valid: boolean
  }[]
}

export async function loadRestorePlanCheck(scope: string): Promise<RestorePlanCheck> {
  const query = new URLSearchParams({ scope }).toString()
  return await fetchJson<RestorePlanCheck>(
    `/api/state-lifecycle/restore-data/plan-check?${query}`,
  )
}

export async function publishCheck(taskId: number): Promise<PublishCheck> {
  return await fetchJson<PublishCheck>(`/api/state-lifecycle/replay/${taskId}/publish-check`)
}
