import os

from app.utils.singleton import Singleton


class Config(metaclass=Singleton):
    """Holds "global" configuration for the application."""
    @staticmethod
    def get_deploy_env() -> str:
        """Get the current deployment environment."""
        return os.environ.get('DEPLOY_ENV', 'prod').upper()
