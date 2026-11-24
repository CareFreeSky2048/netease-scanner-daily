from collections import Counter

import pytest
import engine
import slow_retry


@pytest.fixture
def isolated_retry(monkeypatch):
    monkeypatch.setattr(engine,'CONTROL',engine.ControlState(target_threads=4,baseline_threads=4))
    monkeypatch.setattr(engine,'METRICS',engine.Metrics())
    messages=[]
    monkeypatch.setattr(engine,'emit',messages.append)
    monkeypatch.setattr(slow_retry,'DELAYS',(0,0,0))
    monkeypatch.setattr(slow_retry,'SPACING',0)
    return messages


def test_retries_only_remaining_objects_without_changing_concurrency(isolated_retry):
    calls=Counter()
    def fetch(item):
        calls[item]+=1
        return item=='a' or calls[item]==3
    budget=slow_retry.RetryBudget()
    results=list(slow_retry.retry_failed(['a','b'],fetch,bool,'test',str,budget))
    assert calls=={'a':1,'b':3}
    assert sum(bool(out) for _,out in results)==2
    assert budget.attempted==4 and budget.recovered==2
    assert budget.stages['test']['remaining']==0
    assert engine.CONTROL.snapshot()['target_threads']==4


def test_permanent_failure_has_finite_attempts_and_exception_outcomes(isolated_retry):
    def fetch(item):
        raise ValueError('persistent failure')
    budget=slow_retry.RetryBudget()
    outcomes=list(slow_retry.retry_failed(['a'],fetch,bool,'test',str,budget))
    assert len(outcomes)==3 and all(isinstance(out,ValueError) for _,out in outcomes)
    assert budget.recovered==0 and budget.stages['test']['remaining']==1


def test_stop_interrupts_wait_before_any_request(isolated_retry,monkeypatch):
    monkeypatch.setattr(slow_retry,'DELAYS',(300,))
    def emit(message):
        isolated_retry.append(message)
        engine.CONTROL.stop()
    monkeypatch.setattr(engine,'emit',emit)
    calls=[]
    budget=slow_retry.RetryBudget()
    assert not list(slow_retry.retry_failed(['a'],calls.append,bool,'test',str,budget))
    assert not calls


def test_elapsed_budget_prevents_next_request(isolated_retry,monkeypatch):
    monkeypatch.setattr(slow_retry,'MAX_SECONDS',1)
    times=iter((0,2))
    monkeypatch.setattr(slow_retry.time,'monotonic',lambda:next(times))
    calls=[]
    def fetch(item):
        calls.append(item)
        return False
    budget=slow_retry.RetryBudget()
    list(slow_retry.retry_failed(['a','b'],fetch,bool,'test',str,budget))
    assert calls==['a'] and budget.stages['test']['remaining']==2
