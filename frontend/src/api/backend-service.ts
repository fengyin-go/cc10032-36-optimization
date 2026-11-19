import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 输出批次：重试、封口、完整性报告与检查点核对 ----

export type OutputBatch = {
  batch_id: string
  task_name: string
  sink_name: string
  source: string
  version: string
  status: string
  status_label: string
  sealed: boolean
  complete: boolean
  partition_count: number
  success: number
  failed: number
  abandoned: number
  attempts: number
  retry_count: number
  replaced_count: number
  report_version: number
  created_at: number
  updated_at: number
  deadline_at: number
  sealed_at: number | null
}

export type IntegrityReport = {
  batch_id: string
  source: string
  source_version: string
  report_version: number
  status: string
  status_label: string
  sealed: boolean
  complete: boolean
  expected: number
  success: number
  abandoned: number
  active_failures: number
  failed_partitions: string[]
  abandoned_partitions: string[]
  success_partitions: string[]
  attempts: number
  retry_count: number
  replaced_count: number
  generated_at: number
}

export type ReplacedRecord = {
  attempt: number
  partition: string
  record_id: number | null
  previous_status: string
  previous_error: string | null
  new_status: string
  replaced_at: number
}

export type BatchReceipt = {
  receipt_id: string
  batch_id: string
  status: string
  status_label: string
  complete: boolean
  source: string
  source_version: string
  report_version: number
  success: number
  abandoned: number
  active_failures: number
  attempts: number
  retry_count: number
  replaced_count: number
  abandoned_partitions: string[]
  sealed_at: number
}

export type BatchConfirm = {
  ok: boolean
  batch: OutputBatch
  receipt: BatchReceipt | null
  report: IntegrityReport
  replaced_records: ReplacedRecord[]
  history: Record<string, unknown>[]
}

export async function listOutputBatches(): Promise<{ items: OutputBatch[]; total: number }> {
  const response = await request('/api/output-batches')
  if (!response.ok) throw new Error('输出批次读取失败')
  return await response.json()
}

export async function getOutputBatch(batchId: string): Promise<BatchConfirm> {
  const response = await request(`/api/output-batches/${encodeURIComponent(batchId)}`)
  if (!response.ok) throw new Error('批次详情读取失败')
  return await response.json()
}

export async function retryOutputBatch(batchId: string): Promise<BatchConfirm & { message?: string }> {
  const response = await request(
    `/api/output-batches/${encodeURIComponent(batchId)}/attempts`,
    {
      method: 'POST',
      body: JSON.stringify({ request_id: `ui_${batchId}_${Date.now()}` }),
    },
  )
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? '批次重试失败')
  }
  return await response.json()
}

export async function sealOutputBatch(
  batchId: string,
  abandonFailed = false,
): Promise<{ ok: boolean; batch: OutputBatch; receipt: BatchReceipt; report: IntegrityReport }> {
  const response = await request(
    `/api/output-batches/${encodeURIComponent(batchId)}/seal`,
    {
      method: 'POST',
      body: JSON.stringify({
        abandon_failed: abandonFailed,
        request_id: `ui_seal_${batchId}_${Date.now()}`,
      }),
    },
  )
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? '批次封口失败')
  }
  return await response.json()
}

export async function loadBatchGate(batchId: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/output-batches/${encodeURIComponent(batchId)}/gate`)
  if (!response.ok) throw new Error('检查点闸门读取失败')
  return await response.json()
}
