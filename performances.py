import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def analyze_portfolio_performance(portfolio_df, benchmark_df=None):
    """
    Analyse la performance d'un portefeuille à partir d'un DataFrame contenant les valeurs hebdomadaires.
    
    Args:
        portfolio_df: DataFrame contenant les valeurs du portefeuille avec la colonne 'portfolio_value'
        benchmark_df: (facultatif) DataFrame contenant les valeurs de l'indice de référence, avec la colonne 'benchmark_value'
    """
    portfolio_df['date'] = pd.to_datetime(portfolio_df['date'])
    portfolio_df.sort_values('date', inplace=True)
    portfolio_df = portfolio_df[portfolio_df['date'] <= '2023-12-31']
    
    # Calcul des rendements hebdomadaires
    portfolio_df['return'] = portfolio_df['portfolio_value'].pct_change()
    
    # Calcul des statistiques de performance
    sharpe_ratio = portfolio_df['return'].mean() / portfolio_df['return'].std() * (52 ** 0.5)  # Annualisé
    volatility = portfolio_df['return'].std() * (52 ** 0.5)  # Annualisée
    cumulative_return = (portfolio_df['portfolio_value'].iloc[-1] / portfolio_df['portfolio_value'].iloc[0]) - 1
    
    print("\n=== Analyse des performances ===")
    print(f"Ratio de Sharpe: {sharpe_ratio:.2f}")
    print(f"Volatilité annualisée: {volatility:.2%}")
    print(f"Rendement cumulé: {cumulative_return:.2%}")
    
    # Si un benchmark est fourni, calcul des autres statistiques
    if benchmark_df is not None:
        benchmark_df['date'] = pd.to_datetime(benchmark_df['date'])
        benchmark_df.sort_values('date', inplace=True)
        benchmark_df['return'] = benchmark_df['benchmark_value'].pct_change()

        # Alpha et Beta par rapport au benchmark
        covariance = np.cov(portfolio_df['return'].dropna(), benchmark_df['return'].dropna())[0, 1]
        benchmark_volatility = benchmark_df['return'].std()
        beta = covariance / benchmark_volatility**2
        alpha = portfolio_df['return'].mean() - beta * benchmark_df['return'].mean()
        
        print(f"Alpha: {alpha:.2%}")
        print(f"Beta: {beta:.2f}")
    
    # Maximum Drawdown
    running_max = portfolio_df['portfolio_value'].cummax()
    drawdown = (portfolio_df['portfolio_value'] - running_max) / running_max
    max_drawdown = drawdown.min()
    print(f"Maximum Drawdown: {max_drawdown:.2%}")
    
    # Sortino Ratio
    downside_returns = portfolio_df['return'][portfolio_df['return'] < 0]
    sortino_ratio = portfolio_df['return'].mean() / downside_returns.std() * (52 ** 0.5)
    print(f"Sortino Ratio: {sortino_ratio:.2f}")
    
    # Tracking Error
    if benchmark_df is not None:
        tracking_error = np.std(portfolio_df['return'] - benchmark_df['return'])
        print(f"Tracking Error: {tracking_error:.2%}")
    
    # Tracé de la valeur du portefeuille
    plt.figure(figsize=(10, 5))
    plt.plot(portfolio_df['date'], portfolio_df['portfolio_value'], label='Valeur du portefeuille', color='b')
    plt.xlabel('Date')
    plt.ylabel('Valeur du portefeuille (€)')
    plt.title('Évolution de la valeur du portefeuille')
    plt.legend()
    plt.grid()
    plt.show()
    
