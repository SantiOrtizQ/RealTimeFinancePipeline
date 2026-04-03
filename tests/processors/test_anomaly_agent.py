import pytest
from collections import deque
from processors.anomaly_agent import compute_z_score

def test_z_score_normal_prices():
    prices=deque([
        100.0, 101.0, 99.0, 100.5, 100.2,
        99.8, 100.1, 100.3, 99.9, 100.0,
        100.4, 99.7, 100.2, 100.0, 99.8,
        100.1, 100.3, 99.9, 100.0, 100.2
    ])
    mean, std_dev, z_score=compute_z_score(prices)
    assert abs(z_score)<3.0, "Normal prices should not trigger anomaly"
    assert std_dev>0


def test_z_score_detects_spike():
    prices=deque([100.0]*19+[200.0])
    mean, std_dev, z_score=compute_z_score(prices)
    assert z_score>3.0, "A price spike should produce a high z-score"

def test_z_score_detects_drop():
    prices=deque([100.0]*19+[10.0])
    mean_std_dev, z_score=compute_z_score(prices)
    assert z_score<-3.0, "A price drop should produce a negative z-score"

def test_z_score_flat_prices():
    prices=deque([100.0]*20)
    mean, std_dev, z_score=compute_z_score(prices)
    assert std_dev==0
    assert z_score==0.0, "Flat prices should return z-score of zero"

def test_z_score_mean_is_correct():
    prices=deque([10.0, 20.0, 30.0]+[0.0]*17)
    mean, _, _=compute_z_score(prices)
    expected_mean=sum(prices)/len(prices)
    assert abs(mean-expected_mean)<1e-9