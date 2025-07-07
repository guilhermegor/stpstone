"""Test the Markowitz Effcient Frontier class."""
import numpy as np
import pandas as pd
import pytest

from stpstone.analytics.portfolio_alloc.markowitz_eff import MarkowitzEff


@pytest.fixture
def sample_market_data() -> pd.DataFrame:
    """Generate sample market data for testing."""
    tickers = ['AAPL', 'GOOGL', 'MSFT']
    dates = pd.date_range(start='2020-01-01', end='2020-12-31', freq='D')
    data = []
    for ticker in tickers:
        for date in dates:
            close = np.random.uniform(100.0, 200)
            daily_return = np.random.uniform(-0.05, 0.05)
            data.append({
                'ticker': ticker,
                'dt_date': date,
                'close': close,
                'daily_return': daily_return,
                'min_w': 0.1
            })
    return pd.DataFrame(data)

def test_initialization(sample_market_data: pd.DataFrame) -> None:
    """Test initialization of MarkowitzEff class."""
    markowitz = MarkowitzEff(
        df_mktdata=sample_market_data,
        int_n_portfolios=100,
        float_prtf_notional=10000.0,
        float_rf=0.02,
        bl_debug_mode=False,
        bl_show_plot=False
    )
    assert markowitz is not None
    assert len(markowitz.list_securities) == 3
    assert markowitz.array_returns.shape[0] == 3

def test_random_portfolios(sample_market_data: pd.DataFrame) -> None:
    """Test random portfolio generation."""
    markowitz = MarkowitzEff(
        df_mktdata=sample_market_data,
        int_n_portfolios=100,
        float_prtf_notional=10000.0,
        float_rf=0.02,
        bl_debug_mode=False,
        bl_show_plot=False
    )
    assert len(markowitz.array_mus) == 100
    assert len(markowitz.array_sigmas) == 100
    assert len(markowitz.array_sharpes) == 100
    assert len(markowitz.array_weights) == 100

def test_optimal_portfolios(sample_market_data: pd.DataFrame) -> None:
    """Test optimal portfolio generation."""
    markowitz = MarkowitzEff(
        df_mktdata=sample_market_data,
        int_n_portfolios=100,
        float_prtf_notional=10000.0,
        float_rf=0.02,
        bl_debug_mode=False,
        bl_show_plot=False
    )
    assert len(markowitz.array_eff_returns) > 0
    assert len(markowitz.array_eff_risks) > 0
    assert markowitz.array_eff_weights.shape[0] > 0

def test_max_sharpe(sample_market_data: pd.DataFrame) -> None:
    """Test maximum Sharpe ratio portfolio."""
    markowitz = MarkowitzEff(
        df_mktdata=sample_market_data,
        int_n_portfolios=100,
        float_prtf_notional=10000.0,
        float_rf=0.02,
        bl_debug_mode=False,
        bl_show_plot=False
    )
    max_sharpe = markowitz.max_sharpe()
    assert max_sharpe is not None
    assert 'tickers' in max_sharpe
    assert 'eff_weights' in max_sharpe
    assert 'eff_mu' in max_sharpe
    assert 'eff_sharpe' in max_sharpe
    assert 'eff_quantities' in max_sharpe
    assert 'notional_total' in max_sharpe

def test_min_sigma(sample_market_data: pd.DataFrame) -> None:
    """Test minimum risk portfolio."""
    markowitz = MarkowitzEff(
        df_mktdata=sample_market_data,
        int_n_portfolios=100,
        float_prtf_notional=10000.0,
        float_rf=0.02,
        bl_debug_mode=False,
        bl_show_plot=False
    )
    min_sigma = markowitz.min_sigma()
    assert min_sigma is not None
    assert 'tickers' in min_sigma
    assert 'eff_weights' in min_sigma
    assert 'eff_risk' in min_sigma
    assert 'eff_mu' in min_sigma
    assert 'eff_quantities' in min_sigma
    assert 'notional_total' in min_sigma

def test_plot_risk_return_portfolio(sample_market_data: pd.DataFrame) -> None:
    """Test plotting functionality (just checks if it runs without errors)."""
    markowitz = MarkowitzEff(
        df_mktdata=sample_market_data,
        int_n_portfolios=100,
        float_prtf_notional=10000.0,
        float_rf=0.02,
        bl_debug_mode=False,
        bl_show_plot=False
    )
    # just test that the function runs without errors
    markowitz.plot_risk_return_portfolio()