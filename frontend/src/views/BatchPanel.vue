<template>
  <section class="batch-panel" data-testid="batch-panel">
    <header class="batch-head">
      <div>
        <h3>输出完整性 · 批次封口</h3>
        <p class="page-desc">
          每次重试只替换同一逻辑分区的失败状态；封口后不再追加记录，报告只统计最新成功结果与被放弃分区。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" :disabled="loading" @click="reload">刷新核对</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="batch-error">{{ errorMessage }}</p>

    <div class="batch-layout">
      <ul class="batch-list" data-testid="batch-list">
        <li
          v-for="batch in batches"
          :key="batch.batch_id"
          :class="['batch-item', { active: batch.batch_id === selectedId }]"
          :data-testid="`batch-${batch.batch_id}`"
          @click="select(batch.batch_id)"
        >
          <div class="batch-item-top">
            <strong>{{ batch.task_name }}</strong>
            <span :class="['badge', batch.complete ? 'ok' : batch.sealed ? 'warn' : 'muted']">
              {{ batch.status_label }}
            </span>
          </div>
          <div class="batch-item-meta">
            <span>{{ batch.source }}</span>
            <span>版本 {{ batch.version }}</span>
            <span>重试 {{ batch.retry_count }}</span>
            <span>替换 {{ batch.replaced_count }}</span>
          </div>
        </li>
        <li v-if="!batches.length" class="empty-state">暂无输出批次</li>
      </ul>

      <article v-if="detail" class="batch-detail" data-testid="batch-detail">
        <div class="batch-detail-head">
          <div>
            <h4>{{ detail.batch.task_name }}</h4>
            <p class="page-desc">
              批次 {{ detail.batch.batch_id }} · 来源版本 {{ detail.batch.version }} ·
              报告版本 v{{ detail.report.report_version }}
            </p>
          </div>
          <span :class="['badge', detail.report.complete ? 'ok' : detail.batch.sealed ? 'warn' : 'muted']"
                data-testid="batch-status-label">
            {{ detail.report.status_label }}
          </span>
        </div>

        <div class="stat-row">
          <article class="stat-card">
            <span class="stat-label">逻辑分区</span>
            <strong class="stat-value">{{ detail.report.expected }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">最新成功</span>
            <strong class="stat-value" data-testid="batch-success">{{ detail.report.success }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">被放弃分区</span>
            <strong class="stat-value" data-testid="batch-abandoned">{{ detail.report.abandoned }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">活动失败项</span>
            <strong class="stat-value" data-testid="batch-active-failures">{{ detail.report.active_failures }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">重试次数</span>
            <strong class="stat-value" data-testid="batch-retries">{{ detail.report.retry_count }}</strong>
          </article>
          <article class="stat-card">
            <span class="stat-label">被替换记录</span>
            <strong class="stat-value" data-testid="batch-replaced">{{ detail.report.replaced_count }}</strong>
          </article>
        </div>

        <div class="batch-actions">
          <button
            class="btn"
            type="button"
            data-testid="batch-retry"
            :disabled="busy || detail.batch.sealed || detail.report.active_failures === 0"
            @click="retryBatch"
          >
            重试失败分区
          </button>
          <button
            class="btn primary"
            type="button"
            data-testid="batch-seal-complete"
            :disabled="busy || detail.batch.sealed || detail.report.active_failures > 0"
            @click="sealBatch(false)"
          >
            完整封口
          </button>
          <button
            class="btn"
            type="button"
            data-testid="batch-seal-gap"
            :disabled="busy || detail.batch.sealed || detail.report.active_failures === 0"
            @click="sealBatch(true)"
          >
            放弃失败分区并带缺口封口
          </button>
        </div>

        <section class="batch-block">
          <h5>完整性报告口径</h5>
          <dl class="batch-facts">
            <div><dt>状态</dt><dd>{{ detail.report.status }}</dd></div>
            <div><dt>是否封口</dt><dd>{{ detail.report.sealed ? '是' : '否' }}</dd></div>
            <div><dt>来源</dt><dd>{{ detail.report.source }}</dd></div>
            <div><dt>来源版本</dt><dd>{{ detail.report.source_version }}</dd></div>
            <div><dt>成功分区</dt><dd>{{ detail.report.success_partitions.join('、') || '—' }}</dd></div>
            <div><dt>放弃分区</dt><dd>{{ detail.report.abandoned_partitions.join('、') || '—' }}</dd></div>
            <div><dt>失败分区</dt><dd>{{ detail.report.failed_partitions.join('、') || '—' }}</dd></div>
            <div><dt>检查点闸门</dt><dd>{{ gateSummary }}</dd></div>
          </dl>
        </section>

        <section v-if="detail.receipt" class="batch-block" data-testid="batch-receipt">
          <h5>完成回执</h5>
          <dl class="batch-facts">
            <div><dt>回执号</dt><dd>{{ detail.receipt.receipt_id }}</dd></div>
            <div><dt>封口状态</dt><dd>{{ detail.receipt.status_label }}</dd></div>
            <div><dt>报告版本</dt><dd>v{{ detail.receipt.report_version }}</dd></div>
            <div><dt>重试 / 替换</dt><dd>{{ detail.receipt.retry_count }} / {{ detail.receipt.replaced_count }}</dd></div>
            <div><dt>封口时间</dt><dd>{{ formatTime(detail.receipt.sealed_at) }}</dd></div>
          </dl>
        </section>

        <section class="batch-block">
          <h5>被替换记录</h5>
          <table class="data-table" data-testid="replaced-table">
            <thead>
              <tr><th>轮次</th><th>分区</th><th>记录 ID</th><th>原状态</th><th>新状态</th><th>替换时间</th></tr>
            </thead>
            <tbody>
              <tr v-for="item in detail.replaced_records" :key="`${item.attempt}-${item.partition}`">
                <td>{{ item.attempt }}</td>
                <td>{{ item.partition }}</td>
                <td>{{ item.record_id ?? '—' }}</td>
                <td>{{ item.previous_status }}</td>
                <td>{{ item.new_status }}</td>
                <td>{{ formatTime(item.replaced_at) }}</td>
              </tr>
              <tr v-if="!detail.replaced_records.length">
                <td colspan="6" class="empty-state">没有发生状态替换</td>
              </tr>
            </tbody>
          </table>
        </section>
      </article>
      <article v-else class="batch-detail empty-state">选择左侧批次查看重试与封口情况</article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  getOutputBatch,
  listOutputBatches,
  loadBatchGate,
  retryOutputBatch,
  sealOutputBatch,
} from '@/api/backend-service'
import type { BatchConfirm, OutputBatch } from '@/api/backend-service'

const batches = ref<OutputBatch[]>([])
const selectedId = ref<string>('')
const detail = ref<BatchConfirm | null>(null)
const gate = ref<Record<string, unknown> | null>(null)
const loading = ref(false)
const busy = ref(false)
const errorMessage = ref('')

const gateSummary = computed(() => {
  if (!gate.value) return '—'
  const allowed = Boolean(gate.value.allowed)
  const reasons = (gate.value.reasons as string[] | undefined) ?? []
  return allowed ? '允许下一批复用该版本' : `未放行：${reasons.join('；') || '未知原因'}`
})

function formatTime(value: number | null | undefined): string {
  if (!value) return '—'
  return new Date(value * 1000).toLocaleString('zh-CN', { hour12: false })
}

async function select(batchId: string) {
  selectedId.value = batchId
  await loadDetail(batchId)
}

async function loadDetail(batchId: string) {
  try {
    detail.value = await getOutputBatch(batchId)
    gate.value = await loadBatchGate(batchId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次详情读取失败'
  }
}

async function reload() {
  loading.value = true
  errorMessage.value = ''
  try {
    const payload = await listOutputBatches()
    batches.value = payload.items
    if (!payload.items.length) {
      detail.value = null
      return
    }
    const keep = payload.items.find((item) => item.batch_id === selectedId.value)
    const target = keep ?? payload.items[0]
    selectedId.value = target.batch_id
    await loadDetail(target.batch_id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '输出批次读取失败'
  } finally {
    loading.value = false
  }
}

async function retryBatch() {
  if (!detail.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    await retryOutputBatch(detail.value.batch.batch_id)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次重试失败'
  } finally {
    busy.value = false
  }
}

async function sealBatch(abandonFailed: boolean) {
  if (!detail.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    await sealOutputBatch(detail.value.batch.batch_id, abandonFailed)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次封口失败'
  } finally {
    busy.value = false
  }
}

defineExpose({ reload })

onMounted(reload)
</script>

<style scoped>
.batch-panel { margin-top: 24px; }
.batch-head { display: flex; justify-content: space-between; align-items: flex-start; }
.batch-layout { display: grid; grid-template-columns: 280px 1fr; gap: 16px; margin-top: 10px; }
.batch-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 8px; }
.batch-item { border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; background: #fff; cursor: pointer; }
.batch-item.active { border-color: var(--brand); box-shadow: 0 0 0 1px var(--brand) inset; }
.batch-item-top { display: flex; justify-content: space-between; gap: 8px; font-size: 13px; }
.batch-item-meta { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 6px; font-size: 12px; color: var(--muted); }
.badge { border-radius: 999px; padding: 2px 8px; font-size: 12px; white-space: nowrap; }
.badge.ok { background: #e7f6ec; color: #1a7f37; }
.badge.warn { background: #fff4e5; color: #b25e09; }
.badge.muted { background: #eef1f5; color: var(--muted); }
.batch-detail { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.batch-detail-head { display: flex; justify-content: space-between; align-items: flex-start; }
.batch-actions { display: flex; gap: 8px; margin: 12px 0; flex-wrap: wrap; }
.batch-block { border-top: 1px solid var(--border); padding-top: 10px; margin-top: 10px; }
.batch-block h5 { margin: 0 0 8px; font-size: 13px; }
.batch-facts { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px 16px; margin: 0; }
.batch-facts dt { font-size: 12px; color: var(--muted); }
.batch-facts dd { margin: 0; font-size: 13px; }
</style>
