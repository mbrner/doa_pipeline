import re
from contextlib import contextmanager

from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects import postgresql
from sqlalchemy import cast


def create_engine_context(engine):
    session_engine = sessionmaker(bind=engine)
    
    @contextmanager
    def session_scope():
        """Provide a transactional scope around a series of operations."""
        session = session_engine()
        try:
            yield session
            session.commit()
        except Exception as err:
            session.rollback()
            raise err
        finally:
            session.close()
    
    return session_scope


class ArrayOfEnum(postgresql.ARRAY):
    def bind_expression(self, bindvalue):
        return cast(bindvalue, self)

    def result_processor(self, dialect, coltype):
        super_rp = super(ArrayOfEnum, self).result_processor(dialect, coltype)

        def handle_raw_string(value):
            if value==None:
                return []
            inner = re.match(r"^{(.*)}$", value).group(1)
            return inner.split(",")

        def process(value):
            return super_rp(handle_raw_string(value))
        return process
