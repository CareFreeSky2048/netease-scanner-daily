"""Bounded, serial retries of failed scan objects; shares the scan's control lock."""
import time
from dataclasses import dataclass, field

DELAYS = (30.0, 120.0, 300.0)
SPACING = 1.0
MAX_SECONDS = 1800.0


@dataclass
class RetryBudget:
    spent: float = 0.0
    attempted: int = 0
    recovered: int = 0
    stages: dict = field(default_factory=dict)

    def snapshot(self):
        return {'attempted': self.attempted, 'recovered': self.recovered,
                'elapsed_sec': round(self.spent, 2), 'stages': self.stages}


def _wait(engine, seconds, budget):
    remaining = seconds
    while remaining > 0:
        if not engine.wait_if_paused_or_stopped() or budget.spent >= MAX_SECONDS:
            return False
        start = time.monotonic()
        time.sleep(min(0.25, remaining, MAX_SECONDS - budget.spent))
        elapsed = time.monotonic() - start
        budget.spent += elapsed
        remaining -= elapsed
    return engine.wait_if_paused_or_stopped() and budget.spent < MAX_SECONDS


def retry_failed(items, func, succeeded, stage, item_id, budget):
    """Yield outcomes for failed objects only; never touches configured concurrency."""
    import engine
    pending = list(items)
    if not pending:
        return
    stats = budget.stages.setdefault(stage, {'initial_failed': len(pending), 'attempted': 0, 'recovered': 0, 'remaining': len(pending)})
    for round_no, delay in enumerate(DELAYS, 1):
        if not pending or budget.spent >= MAX_SECONDS:
            break
        engine.METRICS.set_phase('slow_retry')
        engine.METRICS.set_phase_progress(0, len(pending))
        engine.emit({'type': 'log', 'level': 'info', 'text':
                     f'{stage} 低速补扫第 {round_no}/{len(DELAYS)} 轮：剩余 {len(pending)} 项，等待 {delay:g} 秒后单线程重试。'})
        if not _wait(engine, delay, budget):
            break
        next_pending = []
        for index, item in enumerate(pending):
            if not _wait(engine, SPACING if index else 0, budget):
                next_pending.extend(pending[index:])
                break
            start = time.monotonic()
            try:
                outcome = func(item)
                ok = succeeded(outcome)
            except Exception as exc:
                outcome, ok = exc, False
            budget.spent += time.monotonic() - start
            budget.attempted += 1
            stats['attempted'] += 1
            if ok:
                budget.recovered += 1
                stats['recovered'] += 1
            else:
                next_pending.append(item)
            engine.METRICS.set_phase_progress(index + 1, len(pending))
            engine.emit({'type':'log', 'level':'success' if ok else 'error', 'text':
                         f'{stage} 低速补扫 · ID {item_id(item)} · 第 {round_no} 轮：' + ('恢复成功' if ok else '仍失败')})
            engine.emit({'type':'progress','phase':'失败对象低速补扫（单线程）','current':index+1,'total':len(pending),
                         'state':engine.CONTROL.snapshot(),'metrics':engine.METRICS.snapshot()})
            yield item, outcome
        pending = next_pending
        stats['remaining'] = len(pending)
    engine.emit({'type':'log','level':'error' if pending else 'success','text':
                 f'{stage} 低速补扫结束：恢复 {stats["recovered"]} 项，剩余失败 {len(pending)} 项。'})
