import sqlite3
from datetime import datetime, timedelta
import pandas as pd


from data_collector import (
    generate_precise_client,
    generate_random_client,
    manager_affiliation,
    create_manager,
    create_portfolio
)
from base_builder import Client, AssetManager, Portfolio, BaseModel
from strategies import Simulation
from performances import analyze_portfolio_performance

def main() -> None:
    """
    Fonction principale du programme.
    
    Cette fonction gère le menu principal et le flux de contrôle du programme.
    """
    # Création de la base de données si elle n'existe pas
    BaseModel.create_database()
    
    print("\n=== Système de Gestion de Fonds d'Investissement ===")
    print("1. Enregistrer un nouveau client")
    print("2. Analyser les performances")
    print("3. Quitter")
    
    choice = input("\nVotre choix : ")
    
    if choice == "1":
        register_new_client()
    elif choice == "2":
        analyze_performance()
    elif choice == "3":
        print("\nAu revoir !")
    else:
        print("\nChoix invalide. Veuillez réessayer.")
        main()

def register_new_client() -> None:
    """
    Enregistre un nouveau client dans le système.
    
    Cette fonction guide l'utilisateur à travers le processus d'enregistrement d'un nouveau client,
    incluant la création du portefeuille et l'attribution d'un manager.
    """
    try:
        db = BaseModel.get_db_connection()
        sortie = False

        # Demande à l'utilisateur son choix
        choice = input("Voulez-vous créer un client aléatoire (1) ou manuel (2) ? 📌 Entrez 1 ou 2 : ")

        if choice == "1":
            client_data = generate_random_client(db)
        elif choice == "2":
            client_data = generate_precise_client(db)
        else:
            print("❌ Choix invalide ! Par défaut, on crée un client aléatoire.")
            client_data = generate_random_client(db)

        print(client_data)
        
        # Attribution d'un manager
        assigned_manager = manager_affiliation(client_data, db)

        if assigned_manager:
            client_data["manager_id"] = assigned_manager["id"]
            print(f"✅ Manager attribué : {assigned_manager['name']}")
        else:
            print("⚠️ Aucun asset manager opérant dans le pays du client, opérant la stratégie adéquate, "
                  "et/ou ayant le niveau de séniorité adapté au client proposé n'a été trouvé.")
            choice = input("Voulez-vous que le fonds recrute un asset manager adéquat (Oui) ou préférez-vous "
                         "annuler l'enregistrement du client (Non) 📌 Entrez 'Oui' ou 'Non' : ")
            
            if choice == "Oui":
                assigned_manager = create_manager(client_data, db)
            elif choice == "Non":
                client_data = None
                print("❌ Enregistrement du client annulé.")
                sortie = True
            else:
                print("❌ Choix invalide ! Par défaut, on trouve un Asset Manager adapté.")
                assigned_manager = create_manager(client_data, db)
            
            if not sortie:
                print(f"✅ Un Manager a le profil correspondant: {assigned_manager['name']}")
                choice = input("Devons-nous le recruter? 📌 Entrez 'Oui' ou 'Non' : ")

                if choice == "Oui":
                    print(assigned_manager)
                    manager = AssetManager(**assigned_manager)
                    client_data["manager_id"] = manager.save(db)
                    print(f"✅ Manager {manager.name} recruté avec succès.")
                else:
                    print("❌ Enregistrement du client annulé.")
                    sortie = True

        if not sortie:
            print("Le manager crée un portefeuille adapté au client, il doit donc récupérer les actifs du portefeuille")
            # Création du portefeuille
            portfolio_data = create_portfolio(assigned_manager, client_data, db)
            
            if portfolio_data is None:
                print("❌ Impossible de créer le portefeuille. Enregistrement du client annulé.")
                sortie = True
            else:
                print("✅ Portefeuille créé avec succès.")
                
                
                # Création du client
                client = Client(**client_data)
                client_id = client.save(db)
                
                # Mise à jour du portefeuille avec l'ID du client
                portfolio_data["client_id"] = client_id
                portfolio = Portfolio(**portfolio_data)
                portfolio.save(db)
                
                print(f"✅ {client_data['name']} est à présent un(e) client(e) de 'Data Management Project'.")

        db.close()

    except sqlite3.Error as e:
        print(f"❌ Erreur SQLite : {e}")
    except Exception as e:
        print(f"❌ Une erreur inattendue s'est produite : {e}")


def analyze_performance():
    """Fonction pour analyser les performances."""
    print("\n=== Analyse des Performances ===")
    print("1. Analyser un client spécifique")
    print("2. Analyser un manager spécifique")
    print("3. Analyser le fonds globalement")
    print("4. Retour au menu principal")
    
    choice = input("\nVotre choix : ")
    
    if choice == "1":
        analyze_client_performance()
    #elif choice == "2":
    #    analyze_manager_performance()
    #elif choice == "3":
        #analyze_fund_performance()
    elif choice == "4":
        main()
    else:
        print("\nChoix invalide. Veuillez réessayer.")
        analyze_performance()


def analyze_client_performance():
    """Fonction pour analyser les performances d'un client spécifique."""
    db = BaseModel.get_db_connection()
    cursor = db.cursor()
    
    # Récupérer le dernier client inscrit
    cursor.execute("""
        SELECT c.id, c.name, c.registration_date, p.id as portfolio_id, p.strategy
        FROM Clients c
        LEFT JOIN Portfolios p ON c.id = p.client_id
        ORDER BY c.id DESC
        LIMIT 1
    """)
    last_client = cursor.fetchone()
    
    print(f"\nDernier client inscrit : {last_client[1]} (inscrit le {last_client[2]})")
    print("Voulez-vous analyser ce client ? (o/n)")
    choice = input()
    
    if choice.lower() == 'o':
        client_id = last_client[0]
        portfolio_id = last_client[3]
        strategy = last_client[4]
        client_registration_date = last_client[2]
    else:
        print("Entrez l'ID du client à analyser :")
        client_id = int(input())
        
        cursor.execute("""
            SELECT c.registration_date, p.id, p.strategy
            FROM Clients c
            LEFT JOIN Portfolios p ON c.id = p.client_id
            WHERE c.id = ?
        """, (client_id,))
        result = cursor.fetchone()
        if not result:
            print("Client non trouvé.")
            return
        
        client_registration_date = result[0]
        portfolio_id = result[1]
        strategy = result[2]
    
    # Récupérer le montant initial investi
    cursor.execute("SELECT investment_amount FROM Clients WHERE id = ?", (client_id,))
    initial_amount = cursor.fetchone()[0]
    
    print(f"\nAnalyse du portefeuille {portfolio_id} (Stratégie: {strategy})")
    print(f"Début de l'analyse à partir du: {client_registration_date}")
    print(f"Montant initial investi : {initial_amount:,.2f} €")
    
     # Créer une instance de Simulation
    simulation = Simulation(db, portfolio_id, strategy, client_registration_date)
    
    # DataFrame pour stocker les performances du portefeuille
    portfolio_performance_df = pd.DataFrame(columns=["date", "cash", "portfolio_value"])
    
    # Variable pour stocker les tickers (produits uniques) rencontrés
    all_tickers = set()

    # Simuler la gestion active du portefeuille
    current_date = datetime.strptime(client_registration_date, '%Y-%m-%d')
    end_date = datetime(2024, 12, 31)
    
    while current_date <= end_date:
        # Trouver le prochain lundi
        while current_date.weekday() != 0:  # 0 = lundi
            current_date += timedelta(days=1)
        
        # Exécuter la stratégie pour ce lundi
        positions, cash = simulation.execute_strategy(current_date)
        
        # Ajouter les tickers rencontrés à la liste
        for position in positions:
            all_tickers.add(position['ticker'])
        
        # Créer une ligne pour stocker la valeur de chaque produit et la valeur totale du portefeuille
        row = {'date': current_date, 'cash': cash['value'], 'portfolio_value': sum(p['value'] for p in positions) + cash['value']}
        
        # Ajouter les valeurs des produits (tickers) dynamiquement dans le DataFrame
        for ticker in all_tickers:
            ticker_value = sum(p['value'] for p in positions if p['ticker'] == ticker)
            row[ticker] = ticker_value
        
        # Ajouter la ligne au DataFrame
        new_row = pd.DataFrame([row])
        portfolio_performance_df = pd.concat([portfolio_performance_df, new_row], ignore_index=True)
        
        # Passer à la semaine suivante
        current_date += timedelta(days=7)
    
    # Réorganiser le DataFrame pour avoir une colonne pour chaque produit et la valeur totale
    portfolio_performance_df.set_index('date', inplace=True)
    
    # Afficher le DataFrame des performances
    print("\n=== Performance du portefeuille ===")
    print(portfolio_performance_df)
    
    # Analyse des performances
    analyze_portfolio_performance(portfolio_performance_df)
    
    # Afficher le résumé final
    print("\n=== Résumé de la gestion active ===")
    print(f"Période : du {datetime.strptime(client_registration_date, '%Y-%m-%d').strftime('%Y-%m-%d')} au {end_date.strftime('%Y-%m-%d')}")
    print(f"Nombre de semaines : {(end_date - datetime.strptime(client_registration_date, '%Y-%m-%d')).days // 7}")
    
    # Calculer la performance finale
    final_positions, cash = simulation.get_portfolio_positions(portfolio_id, end_date)
    if final_positions:
        portfolio_value = sum(position['value'] for position in final_positions) + cash['value']
        
        # Calculer la performance
        performance = (portfolio_value - initial_amount) / initial_amount * 100
        
        print("\n=== Performance du portefeuille ===")
        print(f"Valeur initiale : {initial_amount:,.2f} €")
        print(f"Valeur finale : {portfolio_value:,.2f} €")
        print(f"Performance : {performance:+.2f}%")
        print(f"Gain/Perte : {(portfolio_value - initial_amount):+,.2f} €")
    
    BaseModel.reinitialize_portfolio(db, portfolio_id)
    db.close()


#def analyze_fund_performance():
#    """Fonction pour analyser les performances globales du fonds."""
#        """Fonction pour analyser les performances d'un client spécifique."""

def analyze_fund_performance():
    """Fonction pour analyser les performances globales du fonds."""
    db = BaseModel.get_db_connection()
    cursor = db.cursor()
    
    # Récupérer tous les portefeuilles actifs
    cursor.execute("""
        SELECT p.id, p.client_id, p.strategy, c.registration_date, c.investment_amount
        FROM Portfolios p
        JOIN Clients c ON p.client_id = c.id
    """
    )
    portfolios = cursor.fetchall()
    
    if not portfolios:
        print("Aucun portefeuille trouvé.")
        return
    
    total_initial_investment = 0
    total_final_value = 0
    end_date = datetime(2024, 12, 31)
    
    for portfolio in portfolios:
        portfolio_id, client_id, strategy, registration_date, initial_amount = portfolio
        print(f"\nAnalyse du portefeuille {portfolio_id} (Client {client_id}, Stratégie: {strategy})")
        
        # Convertir la date
        client_registration_date = datetime.strptime(registration_date, '%Y-%m-%d')
        
        # Créer une simulation pour ce portefeuille
        simulation = Simulation(db, portfolio_id, strategy, registration_date)
        
        # Simuler la gestion active du portefeuille
        current_date = client_registration_date
        while current_date <= end_date:
            while current_date.weekday() != 4:  # Aller au vendredi suivant
                current_date += timedelta(days=1)
            simulation.execute_strategy(current_date)
            current_date += timedelta(days=7)
        
        # Récupérer la valeur finale du portefeuille
        final_positions, cash = simulation.get_portfolio_positions(portfolio_id, end_date)
        portfolio_value = sum(position['value'] for position in final_positions) + cash['value']
        
        # Mise à jour des valeurs agrégées
        total_initial_investment += initial_amount
        total_final_value += portfolio_value
        
        print(f"Valeur initiale : {initial_amount:,.2f} €")
        print(f"Valeur finale : {portfolio_value:,.2f} €")
    
    # Calculer la performance globale du fonds
    if total_initial_investment > 0:
        fund_performance = (total_final_value - total_initial_investment) / total_initial_investment * 100
        
        print("\n=== Performance Globale du Fonds ===")
        print(f"Valeur totale initiale : {total_initial_investment:,.2f} €")
        print(f"Valeur totale finale : {total_final_value:,.2f} €")
        print(f"Performance globale : {fund_performance:+.2f}%")
        print(f"Gain/Perte total(e) : {(total_final_value - total_initial_investment):+,.2f} €")
    
    BaseModel.reinitialize_all_portfolios(db)
    db.close()



if __name__ == "__main__":
    main()

