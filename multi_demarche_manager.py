"""
Gestionnaire de configuration pour le projet multi-démarche DRIEETS.
Toutes les démarches utilisent le même token API (API_TOKEN_DRIEETS).
"""

import os
import json
import re
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

@dataclass
class DemarcheConfig:
    """Configuration d'une démarche."""
    number: int
    name: str
    api_token: str
    api_url: str
    enabled: bool
    sync_config: Dict[str, Any]
    filters: Dict[str, Any]

@dataclass
class SyncResult:
    """Résultat de synchronisation d'une démarche."""
    demarche_number: int
    demarche_name: str
    success: bool
    dossiers_processed: int
    errors: List[str]
    duration_seconds: float

class MultiDemarcheManager:
    """
    Gestionnaire principal pour la synchronisation multi-démarche DRIEETS.
    """

    def __init__(self, config_file: str = "config.json"):
        """
        Initialise le gestionnaire avec un fichier de configuration.

        Args:
            config_file: Chemin vers le fichier de configuration JSON
        """
        load_dotenv()
        self.config_file = config_file
        self.config = self._load_config()
        self.demarches = self._load_demarches()

    def _resolve_env_vars(self, text: str) -> str:
        """
        Résout les variables d'environnement dans une chaîne de caractères.
        Format attendu : ${VAR_NAME}

        Args:
            text: Texte contenant des variables d'environnement

        Returns:
            str: Texte avec les variables résolues
        """

        if not isinstance(text, str):
            return text

        def replace_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, f"${{{var_name}}}")  # Garde la variable si non trouvée

        return re.sub(r'\$\{([^}]+)\}', replace_var, text)

    def _resolve_dict_env_vars(self, data: Any) -> Any:
        """
        Résout récursivement les variables d'environnement dans un dictionnaire.

        Args:
            data: Données à traiter (dict, list, str, etc.)

        Returns:
            Données avec les variables d'environnement résolues
        """
        if isinstance(data, dict):
            return {key: self._resolve_dict_env_vars(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._resolve_dict_env_vars(item) for item in data]
        elif isinstance(data, str):
            return self._resolve_env_vars(data)
        else:
            return data

    def _load_config(self) -> Dict[str, Any]:
        """
        Charge et valide le fichier de configuration.

        Returns:
            dict: Configuration chargée avec variables d'environnement résolues
        """
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # Résoudre les variables d'environnement
            config = self._resolve_dict_env_vars(config)

            # Validation de base
            required_sections = ['grist', 'demarches']
            for section in required_sections:
                if section not in config:
                    raise ValueError(f"Section manquante dans la configuration : {section}")

            return config

        except FileNotFoundError:
            raise FileNotFoundError(f"Fichier de configuration non trouvé : {self.config_file}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Erreur de format JSON dans {self.config_file} : {e}")

    def _load_demarches(self) -> List[DemarcheConfig]:
        """
        Charge les configurations des démarches.

        Returns:
            list: Liste des configurations de démarches
        """
        demarches = []

        for demarche_data in self.config['demarches']:
            # Vérifier que le token a été résolu
            api_token = demarche_data.get('api_token', '')
            if api_token.startswith('${'):
                print(f"⚠️  Attention: Token non résolu pour la démarche {demarche_data['number']}")
                print(f"   Variable d'environnement manquante : {api_token}")
                continue

            demarches.append(DemarcheConfig(
                number=demarche_data['number'],
                name=demarche_data['name'],
                api_token=api_token,
                api_url=demarche_data.get('api_url', 'https://demarche.numerique.gouv.fr/api/v2/graphql'),
                enabled=demarche_data.get('enabled', True),
                sync_config=demarche_data.get('sync_config', {}),
                filters=demarche_data.get('filters', {})
            ))

        return demarches

    def get_enabled_demarches(self) -> List[DemarcheConfig]:
        """
        Retourne la liste des démarches activées.

        Returns:
            list: Liste des démarches activées
        """
        return [d for d in self.demarches if d.enabled]

    def get_demarche_config(self, demarche_number: int) -> Optional[DemarcheConfig]:
        """
        Retourne la configuration pour une démarche donnée.

        Args:
            demarche_number: Numéro de la démarche

        Returns:
            DemarcheConfig ou None si non trouvé
        """
        for demarche in self.demarches:
            if demarche.number == demarche_number:
                return demarche
        return None

    def get_grist_config(self) -> Dict[str, str]:
        """
        Retourne la configuration Grist.

        Returns:
            dict: Configuration Grist
        """
        grist_config = self.config['grist'].copy()

        # Vérifier que les variables ont été résolues
        for key, value in grist_config.items():
            if isinstance(value, str) and value.startswith('${'):
                print(f"⚠️  Attention: Variable Grist non résolue : {key} = {value}")

        return grist_config

    def set_environment_for_demarche(self, demarche_number: int) -> bool:
        """
        Configure les variables d'environnement pour une démarche spécifique.

        Args:
            demarche_number: Numéro de la démarche

        Returns:
            bool: True si la configuration a réussi, False sinon
        """
        demarche_config = self.get_demarche_config(demarche_number)
        if not demarche_config:
            print(f"❌ Démarche {demarche_number} non trouvée dans la configuration")
            return False

        # Vérifier que le token est valide
        if not demarche_config.api_token or demarche_config.api_token.startswith('${'):
            print(f"❌ Token API invalide pour la démarche {demarche_number}")
            return False

        print(f"🔄 Reconfiguration pour la démarche {demarche_number}...")
        print(f"   Token: {demarche_config.api_token[:8]}...{demarche_config.api_token[-8:]}")

        # Configurer les variables d'environnement pour l'API DS
        os.environ['DEMARCHES_API_TOKEN'] = demarche_config.api_token
        os.environ['DEMARCHES_API_URL'] = demarche_config.api_url
        os.environ['DEMARCHE_NUMBER'] = str(demarche_number)

        # IMPORTANT : Forcer la mise à jour du cache dans queries_config
        try:
            import queries_config
            queries_config.API_TOKEN = demarche_config.api_token
            queries_config.API_URL = demarche_config.api_url

            # Si la classe DemarcheAPIConfig existe, l'utiliser
            if hasattr(queries_config, 'DemarcheAPIConfig'):
                queries_config.DemarcheAPIConfig.set_current_api_config(
                    demarche_config.api_token,
                    demarche_config.api_url
                )

            # FORCER LE RECHARGEMENT DES MODULES CRITIQUES
            import importlib
            import sys

            # Recharger queries_config pour forcer la prise en compte du nouveau token
            if 'queries_config' in sys.modules:
                importlib.reload(queries_config)
                queries_config.API_TOKEN = demarche_config.api_token
                queries_config.API_URL = demarche_config.api_url

            # Recharger queries_graphql qui utilise le token
            if 'queries_graphql' in sys.modules:
                import queries_graphql
                importlib.reload(queries_graphql)

            # Recharger schema_utils qui utilise aussi le token
            if 'schema_utils' in sys.modules:
                import schema_utils
                importlib.reload(schema_utils)

            print(f"   🔄 Modules rechargés avec le nouveau token")

        except ImportError:
            pass

        # Forcer le rechargement de la dotenv si elle est en cache
        try:
            from dotenv import load_dotenv
            load_dotenv(override=True)  # override=True force le rechargement
        except ImportError:
            pass

        # Configurer les variables Grist
        grist_config = self.get_grist_config()
        os.environ['GRIST_BASE_URL'] = grist_config['base_url']
        os.environ['GRIST_API_KEY'] = grist_config['api_key']
        os.environ['GRIST_DOC_ID'] = grist_config['doc_id']

        # Configurer les filtres
        filters = demarche_config.filters
        os.environ['DATE_DEPOT_DEBUT'] = filters.get('date_depot_debut', '')
        os.environ['DATE_DEPOT_FIN'] = filters.get('date_depot_fin', '')
        os.environ['STATUTS_DOSSIERS'] = ','.join(filters.get('statuts_dossiers', []))
        os.environ['GROUPES_INSTRUCTEURS'] = ','.join(str(g) for g in filters.get('groupes_instructeurs', []))

        # Configurer les paramètres de synchronisation
        sync_config = demarche_config.sync_config
        os.environ['BATCH_SIZE'] = str(sync_config.get('batch_size', 50))
        os.environ['MAX_WORKERS'] = str(sync_config.get('max_workers', 3))
        os.environ['PARALLEL'] = str(sync_config.get('parallel', True)).lower()

        print(f"✅ Environnement configuré pour la démarche {demarche_number} - {demarche_config.name}")

        # VALIDATION : Vérifier que le token est bien appliqué
        import requests
        headers = {
            "Authorization": f"Bearer {demarche_config.api_token}",
            "Content-Type": "application/json"
        }

        # Test simple pour vérifier l'accès
        test_query = """
        query testAccess($demarcheNumber: Int!) {
            demarche(number: $demarcheNumber) {
                id
                title
            }
        }
        """

        try:
            response = requests.post(
                demarche_config.api_url,
                json={"query": test_query, "variables": {"demarcheNumber": demarche_number}},
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                result = response.json()
                if result.get("data") and result["data"].get("demarche"):
                    print(f"   ✅ Token validé - Accès à la démarche confirmé")
                    return True
                else:
                    print(f"   ⚠️  Token configuré mais démarche inaccessible")
                    if "errors" in result:
                        for error in result["errors"]:
                            print(f"      Erreur API: {error.get('message', 'Unknown')}")
                    return True  # Continuer quand même
            else:
                print(f"   ⚠️  Erreur HTTP lors du test: {response.status_code}")
                return True  # Continuer quand même

        except Exception as e:
            print(f"   ⚠️  Impossible de valider le token: {str(e)}")
            return True  # Continuer quand même

    def sync_all_demarches(self) -> List[SyncResult]:
        """
        Synchronise toutes les démarches activées.

        Returns:
            list: Liste des résultats de synchronisation
        """
        results = []
        enabled_demarches = self.get_enabled_demarches()

        print(f"🚀 Démarrage de la synchronisation de {len(enabled_demarches)} démarches")

        for i, demarche in enumerate(enabled_demarches, 1):
            print(f"\n📋 Synchronisation {i}/{len(enabled_demarches)}: {demarche.name} (#{demarche.number})")

            # Configurer l'environnement pour cette démarche
            if not self.set_environment_for_demarche(demarche.number):
                results.append(SyncResult(
                    demarche_number=demarche.number,
                    demarche_name=demarche.name,
                    success=False,
                    dossiers_processed=0,
                    errors=["Échec de la configuration de l'environnement"],
                    duration_seconds=0
                ))
                continue

            # Exécuter la synchronisation
            result = self._sync_single_demarche(demarche)
            results.append(result)

            # Pause entre les démarches pour éviter les problèmes de cache
            if i < len(enabled_demarches):
                print(f"⏸️  Pause de 2 secondes avant la démarche suivante...")
                time.sleep(2)

        # Afficher le résumé
        self._print_sync_summary(results)

        return results

    def sync_specific_demarches(self, demarche_numbers: List[int], force_disabled: bool = False) -> List[SyncResult]:
        """
        Synchronise des démarches spécifiques.

        Args:
            demarche_numbers: Liste des numéros de démarches à synchroniser
            force_disabled: Si True, synchronise même les démarches désactivées

        Returns:
            list: Liste des résultats de synchronisation
        """
        results = []

        print(f"🔍 Recherche des démarches : {demarche_numbers}")

        for demarche_number in demarche_numbers:
            demarche_config = self.get_demarche_config(demarche_number)

            if not demarche_config:
                print(f"❌ Démarche {demarche_number} non trouvée dans la configuration")
                print(f"   Démarches disponibles : {[d.number for d in self.demarches]}")
                results.append(SyncResult(
                    demarche_number=demarche_number,
                    demarche_name=f"Démarche {demarche_number}",
                    success=False,
                    dossiers_processed=0,
                    errors=["Démarche non trouvée dans la configuration"],
                    duration_seconds=0
                ))
                continue

            if not demarche_config.enabled and not force_disabled:
                print(f"⚠️  Démarche {demarche_number} ({demarche_config.name}) désactivée, ignorée")
                print(f"   Utilisez --force pour forcer la synchronisation")
                continue

            print(f"\n📋 Synchronisation: {demarche_config.name} (#{demarche_number})")

            # Configurer l'environnement pour cette démarche
            if not self.set_environment_for_demarche(demarche_number):
                results.append(SyncResult(
                    demarche_number=demarche_number,
                    demarche_name=demarche_config.name,
                    success=False,
                    dossiers_processed=0,
                    errors=["Échec de la configuration de l'environnement"],
                    duration_seconds=0
                ))
                continue

            # Exécuter la synchronisation
            result = self._sync_single_demarche(demarche_config)
            results.append(result)

            # Pause entre les démarches pour éviter les problèmes de cache
            if demarche_number != demarche_numbers[-1]:  # Pas de pause après la dernière
                print(f"⏸️  Pause de 2 secondes avant la démarche suivante...")
                time.sleep(2)

        # Afficher le résumé
        if results:
            self._print_sync_summary(results)
        else:
            print("⚠️  Aucune démarche n'a été synchronisée")

        return results

    def _sync_single_demarche(self, demarche: DemarcheConfig) -> SyncResult:
        """
        Synchronise une seule démarche.

        Args:
            demarche: Configuration de la démarche

        Returns:
            SyncResult: Résultat de la synchronisation
        """
        start_time = time.time()

        try:
            # Importer ici pour éviter les problèmes de dépendances circulaires
            from grist_processor_working_all import GristClient, process_demarche_for_grist_optimized

            # Créer le client Grist
            grist_config = self.get_grist_config()
            client = GristClient(
                grist_config['base_url'],
                grist_config['api_key'],
                grist_config['doc_id']
            )

            # Obtenir les paramètres de synchronisation
            sync_config = demarche.sync_config
            parallel = sync_config.get('parallel', True)
            batch_size = sync_config.get('batch_size', 50)
            max_workers = sync_config.get('max_workers', 3)

            # Exécuter la synchronisation
            success = process_demarche_for_grist_optimized(
                client,
                demarche.number,
                parallel=parallel,
                batch_size=batch_size,
                max_workers=max_workers
            )

            duration = time.time() - start_time

            return SyncResult(
                demarche_number=demarche.number,
                demarche_name=demarche.name,
                success=success,
                dossiers_processed=0,  # À améliorer : récupérer le nombre réel
                errors=[],
                duration_seconds=duration
            )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Erreur lors de la synchronisation : {str(e)}"
            print(f"❌ {error_msg}")

            return SyncResult(
                demarche_number=demarche.number,
                demarche_name=demarche.name,
                success=False,
                dossiers_processed=0,
                errors=[error_msg],
                duration_seconds=duration
            )

    def _print_sync_summary(self, results: List[SyncResult]):
        """
        Affiche un résumé des résultats de synchronisation.

        Args:
            results: Liste des résultats
        """
        print(f"\n{'='*60}")
        print("📊 RÉSUMÉ DE LA SYNCHRONISATION")
        print(f"{'='*60}")

        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        print(f"✅ Synchronisations réussies : {len(successful)}")
        print(f"❌ Synchronisations échouées : {len(failed)}")
        print(f"⏱️  Durée totale : {sum(r.duration_seconds for r in results):.1f} secondes")

        if successful:
            print(f"\n🎉 Démarches synchronisées avec succès :")
            for result in successful:
                print(f"   • {result.demarche_name} (#{result.demarche_number}) - {result.duration_seconds:.1f}s")

        if failed:
            print(f"\n💥 Démarches en échec :")
            for result in failed:
                print(f"   • {result.demarche_name} (#{result.demarche_number})")
                for error in result.errors:
                    print(f"     - {error}")

    def validate_configuration(self) -> bool:
        """
        Valide la configuration complète.

        Returns:
            bool: True si la configuration est valide
        """
        print("🔍 Validation de la configuration...")

        valid = True

        # Vérifier Grist
        grist_config = self.get_grist_config()
        for key in ['base_url', 'api_key', 'doc_id']:
            if not grist_config.get(key) or grist_config[key].startswith('${'):
                print(f"❌ Configuration Grist incomplète : {key}")
                valid = False

        if valid:
            print(f"✅ Configuration Grist valide")

        # Vérifier les démarches
        enabled_count = len(self.get_enabled_demarches())
        total_count = len(self.demarches)
        print(f"📋 Démarches : {enabled_count}/{total_count} activées")

        for demarche in self.demarches:
            if not demarche.api_token or demarche.api_token.startswith('${'):
                print(f"❌ Token manquant pour la démarche {demarche.number} - {demarche.name}")
                valid = False
            else:
                status = "✅ activée" if demarche.enabled else "⚪ désactivée"
                print(f"   {status} - {demarche.name} (#{demarche.number}) - Token configuré")

        if valid:
            print("✅ Configuration globale valide")
        else:
            print("❌ Configuration invalide")

        return valid

def main():
    """
    Point d'entrée principal pour la synchronisation multi-démarche.
    """
    import argparse

    parser = argparse.ArgumentParser(description='Synchronisation multi-démarche DRIEETS Île-de-France vers Grist')
    parser.add_argument('--demarches', type=str, help='Numéros de démarches séparés par des virgules (ex: 135754,135749)')
    parser.add_argument('--force', action='store_true', help='Forcer la synchronisation des démarches désactivées')
    parser.add_argument('--validate-only', action='store_true', help='Valider la configuration uniquement')
    parser.add_argument('--dry-run', action='store_true', help='Mode test (validation uniquement)')
    parser.add_argument('--config', type=str, default='config.json', help='Fichier de configuration')
    parser.add_argument('--debug', action='store_true', help='Activer les logs de debug')

    args = parser.parse_args()

    # Activer le debug si demandé
    if args.debug:
        os.environ['LOG_LEVEL'] = 'DEBUG'
        print("🐛 Mode debug activé")

    try:
        print(f"🚀 Démarrage du gestionnaire multi-démarche DRIEETS Île-de-France")
        print(f"📁 Fichier de configuration : {args.config}")

        # Vérifier que le fichier de configuration existe
        if not os.path.exists(args.config):
            print(f"❌ Fichier de configuration non trouvé : {args.config}")
            print(f"💡 Créez le fichier {args.config} avec vos démarches")
            return 1

        # Initialiser le gestionnaire
        manager = MultiDemarcheManager(args.config)
        print(f"✅ Configuration chargée : {len(manager.demarches)} démarches trouvées")

        # Afficher les démarches disponibles en mode debug
        if args.debug:
            print(f"📋 Démarches disponibles :")
            for d in manager.demarches:
                status = "✅ activée" if d.enabled else "⚪ désactivée"
                token_ok = "🔑 OK" if d.api_token and not d.api_token.startswith('${') else "❌ token manquant"
                print(f"   {d.number}: {d.name} - {status} - {token_ok}")

        # Mode validation uniquement
        if args.validate_only or args.dry_run:
            if manager.validate_configuration():
                print("✅ Configuration valide")
                return 0
            else:
                print("❌ Configuration invalide")
                return 1

        # Valider la configuration avant synchronisation
        if not manager.validate_configuration():
            print("❌ Configuration invalide. Arrêt du programme.")
            return 1

        # Synchronisation spécifique ou complète
        if args.demarches:
            # Synchroniser des démarches spécifiques
            try:
                # Parser les numéros de démarches en gérant les espaces
                demarche_numbers = []
                for x in args.demarches.split(','):
                    cleaned = x.strip()
                    if cleaned:
                        demarche_numbers.append(int(cleaned))

                print(f"🎯 Démarches sélectionnées : {demarche_numbers}")
                results = manager.sync_specific_demarches(demarche_numbers, force_disabled=args.force)
            except ValueError as e:
                print(f"❌ Erreur dans les numéros de démarches : {args.demarches}")
                print(f"   Format attendu : 135754,135749 (sans espaces)")
                print(f"   Votre saisie : '{args.demarches}'")
                return 1
        else:
            # Synchroniser toutes les démarches activées
            results = manager.sync_all_demarches()

        # Vérifier si au moins une synchronisation a réussi
        success_count = sum(1 for r in results if r.success)
        if success_count > 0:
            print(f"\n🎉 Synchronisation terminée : {success_count} démarches traitées avec succès")
            return 0
        else:
            print(f"\n💥 Aucune synchronisation réussie")
            return 1

    except Exception as e:
        print(f"💥 Erreur fatale : {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        else:
            print("💡 Utilisez --debug pour plus de détails")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
