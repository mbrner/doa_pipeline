import re
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker 
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
        except:
            session.rollback()
            raise
        finally:
            session.close()
    
    return session_scope


class ArrayOfEnum(ARRAY):
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
