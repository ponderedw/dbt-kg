import os

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from app.utils.singleton import Singleton


class Database(metaclass=Singleton):
    """Represents the main database.
    Currently, provides only the connection string to the database.
    """

    def __init__(self):
        """Initialize the database connection."""

        super().__init__()

        self.user = os.environ.get('POSTGRES_USER', 'postgres')
        self.password = os.environ['POSTGRES_PASSWORD']
        self.host = os.environ.get('POSTGRES_HOSTNAME', 'postgres')
        self.port = os.environ.get('POSTGRES_PORT', 5432)
        self.database = os.environ.get('POSTGRES_DB', 'chat_db')
        auth = f'{self.user}:{self.password}'
        self.uri = \
            f'postgres://{auth}@{self.host}:{self.port}/{self.database}'

    def get_connection_string(self) -> str:
        """Get a URI representation of the database connection params."""
        return self.uri

    @staticmethod
    async def setup():
        """Setup the database."""
        async with AsyncPostgresSaver.from_conn_string(
           Database().get_connection_string()) as saver:
            await saver.setup()
