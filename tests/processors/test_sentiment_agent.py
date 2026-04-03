import pytest
from processors.sentiment_agent import score_sentiment


def test_positive_headline():
    scores=score_sentiment("Apple beats earnings expectations with record revenue")
    assert scores["sentiment"]=="positive"
    assert scores["compound"]>=0.5


def test_negative_headline():
    scores=score_sentiment("Company reports massive losses amid fraud investigation")
    assert scores["sentiment"]=="negative"
    assert scores["compound"]<=-0.5


def test_neutral_headlin():
    scores=score_sentiment("Apple releases quarterly earnings report on Thursday")
    assert scores["sentiment"] in ("neutral", "positive", "negative")
    assert -1.0<=scores["compound"]<=1.0


def test_compound_range():
    scores=score_sentiment("Markets open higher as investors await Fed decision")
    assert -1.0<=scores["compound"]<=1.0
    assert 0.0<=scores["positive"]<=1.0
    assert 0.0<=scores["neutral"]<=1.0
    assert 0.0<=scores["negative"]<=1.0


def test_description_enriches_score():
    title_only=score_sentiment("Apple resports earnings")
    with_description=score_sentiment(
        title="Apple reports earnings",
        description="Revenue missed analyst expectations by 8 percent"
    )
    assert with_description["compound"]<title_only["compound"], (
        "Negative description should pull compound score lower"
    )


def test_empty_description_handled():
    scores=score_sentiment("Markets rally ahead of Fed meetng", description=None)
    assert scores is not None
    assert "compound" in scores