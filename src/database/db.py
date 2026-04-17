"""
SQLAlchemy database initialization for Flask.
Provides a centralized database engine and session management.
"""
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from config.Config import getServerConfig

_engine = None

def get_db_engine():
  """
  Get or create the SQLAlchemy engine for QuestDB.
  Uses configuration from config/server.json.
  """
  global _engine
  if _engine is None:
    server_config = getServerConfig()
    qdb_config = server_config['questDB']
    connection_string = (
      f"postgresql://{qdb_config['username']}:{qdb_config['password']}@"
      f"{qdb_config['host']}:{qdb_config['port']}/{qdb_config['database']}"
    )
    _engine = create_engine(connection_string, echo=False, pool_pre_ping=True)
  return _engine

def execute_query(query, params=None):
  """
  Execute a raw SQL query and return results.

  Args:
    query: SQL query string
    params: Dictionary of query parameters

  Returns:
    Result proxy object
  """
  engine = get_db_engine()
  with engine.connect() as connection:
    if params:
      return connection.execute(query, params)
    return connection.execute(query)

def get_connection():
  """
  Get a raw database connection from the connection pool.
  Use this for psycopg2-style operations.

  Returns:
    A database connection object
  """
  engine = get_db_engine()
  return engine.raw_connection()
