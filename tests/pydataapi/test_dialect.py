import datetime
from typing import List

import pytest
from sqlalchemy import Column, Integer, String
from sqlalchemy.engine import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


@pytest.fixture
def mocked_client(mocker):
    return mocker.patch('boto3.client')


def test_mysql(mocked_client) -> None:
    from sqlalchemy.dialects.mysql.base import DATETIME, TIMESTAMP

    class Pets(declarative_base()):
        __tablename__ = 'pets'
        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(String(255, collation='utf8_unicode_ci'), default=None)
        first_time = Column(DATETIME)
        updated = Column(TIMESTAMP)
        created = Column(DATETIME)

    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    mocked_client.execute_statement.side_effect = [
        {
            "columnMetadata": [
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": False,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "Variable_name",
                    "name": "VARIABLE_NAME",
                    "nullable": 0,
                    "precision": 256,
                    "scale": 0,
                    "tableName": "VARIABLES",
                    "type": 12,
                    "typeName": "VARCHAR",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": False,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "Value",
                    "name": "VARIABLE_VALUE",
                    "nullable": 1,
                    "precision": 4096,
                    "scale": 0,
                    "tableName": "VARIABLES",
                    "type": 12,
                    "typeName": "VARCHAR",
                },
            ],
            "numberOfRecordsUpdated": 0,
            "records": [
                [{"stringValue": "character_set_client"}, {"stringValue": "utf8mb4"}]
            ],
        },
        {
            'columnMetadata': [
                {
                    'arrayBaseColumnType': 0,
                    'isAutoIncrement': False,
                    'isCaseSensitive': False,
                    'isCurrency': False,
                    'isSigned': True,
                    'label': 'Variable_name',
                    'name': 'VARIABLE_NAME',
                    'nullable': 0,
                    'precision': 256,
                    'scale': 0,
                    'tableName': 'VARIABLES',
                    'type': 12,
                    'typeName': 'VARCHAR',
                },
                {
                    'arrayBaseColumnType': 0,
                    'isAutoIncrement': False,
                    'isCaseSensitive': False,
                    'isCurrency': False,
                    'isSigned': True,
                    'label': 'Value',
                    'name': 'VARIABLE_VALUE',
                    'nullable': 1,
                    'precision': 4096,
                    'scale': 0,
                    'tableName': 'VARIABLES',
                    'type': 12,
                    'typeName': 'VARCHAR',
                },
            ],
            'numberOfRecordsUpdated': 0,
            'records': [
                [
                    {'stringValue': 'sql_mode'},
                    {
                        'stringValue': 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'
                    },
                ]
            ],
        },
        {
            'columnMetadata': [
                {
                    'arrayBaseColumnType': 0,
                    'isAutoIncrement': False,
                    'isCaseSensitive': False,
                    'isCurrency': False,
                    'isSigned': True,
                    'label': 'Variable_name',
                    'name': 'VARIABLE_NAME',
                    'nullable': 0,
                    'precision': 256,
                    'scale': 0,
                    'tableName': 'VARIABLES',
                    'type': 12,
                    'typeName': 'VARCHAR',
                },
                {
                    'arrayBaseColumnType': 0,
                    'isAutoIncrement': False,
                    'isCaseSensitive': False,
                    'isCurrency': False,
                    'isSigned': True,
                    'label': 'Value',
                    'name': 'VARIABLE_VALUE',
                    'nullable': 1,
                    'precision': 4096,
                    'scale': 0,
                    'tableName': 'VARIABLES',
                    'type': 12,
                    'typeName': 'VARCHAR',
                },
            ],
            'numberOfRecordsUpdated': 0,
            'records': [
                [{'stringValue': 'lower_case_table_names'}, {'stringValue': '0'}]
            ],
        },
        {
            'columnMetadata': [
                {
                    'arrayBaseColumnType': 0,
                    'isAutoIncrement': False,
                    'isCaseSensitive': False,
                    'isCurrency': False,
                    'isSigned': True,
                    'label': 'VERSION()',
                    'name': 'VERSION()',
                    'nullable': 0,
                    'precision': 24,
                    'scale': 31,
                    'tableName': '',
                    'type': 12,
                    'typeName': 'VARCHAR',
                }
            ],
            'numberOfRecordsUpdated': 0,
            'records': [[{'stringValue': '5.6.45'}]],
        },
        {
            'columnMetadata': [
                {
                    'arrayBaseColumnType': 0,
                    'isAutoIncrement': False,
                    'isCaseSensitive': False,
                    'isCurrency': False,
                    'isSigned': True,
                    'label': 'DATABASE()',
                    'name': 'DATABASE()',
                    'nullable': 1,
                    'precision': 136,
                    'scale': 31,
                    'tableName': '',
                    'type': 12,
                    'typeName': 'VARCHAR',
                }
            ],
            'numberOfRecordsUpdated': 0,
            'records': [[{'stringValue': 'test'}]],
        },
        {
            'columnMetadata': [
                {
                    'arrayBaseColumnType': 0,
                    'isAutoIncrement': False,
                    'isCaseSensitive': False,
                    'isCurrency': False,
                    'isSigned': True,
                    'label': '@@tx_isolation',
                    'name': '@@tx_isolation',
                    'nullable': 1,
                    'precision': 60,
                    'scale': 31,
                    'tableName': '',
                    'type': 12,
                    'typeName': 'VARCHAR',
                }
            ],
            'numberOfRecordsUpdated': 0,
            'records': [[{'stringValue': 'REPEATABLE-READ'}]],
        },
        {'records': [[{'stringValue': 'test plain returns'}]]},
        {'records': [[{'stringValue': 'test unicode returns'}]]},
        {
            'numberOfRecordsUpdated': 0,
            'records': [
                [
                    {'longValue': 1},
                    {'stringValue': 'cat'},
                    {"stringValue": "2019-11-12 10:20:20.123456"},
                    {"stringValue": 1574706700.170858},
                    {"stringValue": "2019-11-12 10:20:30"},
                ]
            ],
            "columnMetadata": [
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": False,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "id",
                    "name": "id",
                    "nullable": 1,
                    "precision": 11,
                    "scale": 0,
                    "schemaName": "",
                    "tableName": "users",
                    "type": 4,
                    "typeName": "INT",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": False,
                    "isCurrency": False,
                    "isSigned": False,
                    "label": "name",
                    "name": "name",
                    "nullable": 1,
                    "precision": 255,
                    "scale": 0,
                    "schemaName": "",
                    "tableName": "users",
                    "type": 12,
                    "typeName": "VARCHAR",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": True,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "first_time",
                    "name": "first_time",
                    "nullable": 1,
                    "precision": 19,
                    "scale": 0,
                    "tableName": "pets",
                    "type": 93,
                    "typeName": "DATETIME",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": True,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "updated",
                    "name": "updated",
                    "nullable": 1,
                    "precision": 19,
                    "scale": 0,
                    "tableName": "pets",
                    "type": 93,
                    "typeName": "DATETIME",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": True,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "created",
                    "name": "created",
                    "nullable": 1,
                    "precision": 19,
                    "scale": 0,
                    "tableName": "pets",
                    "type": 93,
                    "typeName": "DATETIME",
                },
            ],
        },
        {"generatedFields": [], "numberOfRecordsUpdated": 1},
        {"generatedFields": [], "numberOfRecordsUpdated": 1},
        {"generatedFields": [], "numberOfRecordsUpdated": 1},
    ]
    engine = create_engine(
        'mysql+pydataapi://',
        echo=True,
        connect_args={
            'resource_arn': 'arn:aws:rds:us-east-1:123456789012:cluster:dummy',
            'secret_arn': 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy',
            'database': 'test',
            'client': mocked_client,
        },
    )
    Session = sessionmaker(bind=engine)
    session = Session()
    result: List[Pets] = session.query(Pets).all()
    assert len(result) == 1
    assert result[0].id == 1
    assert result[0].name == 'cat'
    assert result[0].first_time == datetime.datetime(2019, 11, 12, 10, 20, 20, 123456)
    # assert result[0].updated == datetime.datetime(2019, 11, 26, 3, 31, 40, 170858)
    assert result[0].created == datetime.datetime(2019, 11, 12, 10, 20, 30)

    pet = Pets(id=2, name='dog', created=datetime.datetime(2019, 11, 13, 10, 20, 30))
    session.add(pet)
    session.flush()

    pet = Pets(id=3, name='snake', created='2019-11-14 10:20:30')
    session.add(pet)
    session.flush()


def test_postgresql(mocked_client) -> None:
    from sqlalchemy.dialects.postgresql import DATE, TIMESTAMP

    class Pets(declarative_base()):
        __tablename__ = 'pets'
        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(String(255, collation='utf8_unicode_ci'), default=None)
        birthday = Column(DATE)
        first_time = Column(TIMESTAMP)
        updated = Column(TIMESTAMP)
        created = Column(TIMESTAMP)

    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}

    mocked_client.execute_statement.side_effect = [
        {
            'records': [
                [
                    {
                        'stringValue': 'PostgreSQL 10.7 on x86_64-pc-linux-musl, compiled by gcc (Alpine 8.3.0) 8.3.0, 64-bit'
                    }
                ]
            ],
            "columnMetadata": [
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": False,
                    "isCurrency": False,
                    "isSigned": False,
                    "label": "name",
                    "name": "name",
                    "nullable": 1,
                    "precision": 255,
                    "scale": 0,
                    "schemaName": "",
                    "tableName": "users",
                    "type": 12,
                    "typeName": "VARCHAR",
                }
            ],
        },
        {
            'columnMetadata': [
                {
                    'arrayBaseColumnType': 0,
                    'isAutoIncrement': False,
                    'isCaseSensitive': True,
                    'isCurrency': False,
                    'isSigned': False,
                    'label': 'current_schema',
                    'name': 'current_schema',
                    'nullable': 2,
                    'precision': 2147483647,
                    'scale': 0,
                    'tableName': '',
                    'type': 12,
                    'typeName': 'name',
                }
            ],
            'numberOfRecordsUpdated': 0,
            'records': [[{'stringValue': 'public'}]],
        },
        {'records': [[{'stringValue': 'test plain returns'}]]},
        {'records': [[{'stringValue': 'test unicode returns'}]]},
        {
            'columnMetadata': [
                {
                    'arrayBaseColumnType': 0,
                    'isAutoIncrement': False,
                    'isCaseSensitive': True,
                    'isCurrency': False,
                    'isSigned': False,
                    'label': 'transaction_isolation',
                    'name': 'transaction_isolation',
                    'nullable': 2,
                    'precision': 2147483647,
                    'scale': 0,
                    'tableName': '',
                    'type': 12,
                    'typeName': 'text',
                }
            ],
            'numberOfRecordsUpdated': 0,
            'records': [[{'stringValue': 'read committed'}]],
        },
        {
            'columnMetadata': [
                {
                    'arrayBaseColumnType': 0,
                    'isAutoIncrement': False,
                    'isCaseSensitive': True,
                    'isCurrency': False,
                    'isSigned': False,
                    'label': 'standard_conforming_strings',
                    'name': 'standard_conforming_strings',
                    'nullable': 2,
                    'precision': 2147483647,
                    'scale': 0,
                    'tableName': '',
                    'type': 12,
                    'typeName': 'text',
                }
            ],
            'numberOfRecordsUpdated': 0,
            'records': [[{'stringValue': 'on'}]],
        },
        {
            'numberOfRecordsUpdated': 0,
            'records': [
                [
                    {'longValue': 1},
                    {'stringValue': 'cat'},
                    {"stringValue": "2019-11-11"},
                    {"stringValue": "2019-11-12 10:20:20.123456"},
                    {"stringValue": 1574706700.170858},
                    {"stringValue": "2019-11-12 10:20:30"},
                ]
            ],
            "columnMetadata": [
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": False,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "id",
                    "name": "id",
                    "nullable": 1,
                    "precision": 11,
                    "scale": 0,
                    "schemaName": "",
                    "tableName": "users",
                    "type": 4,
                    "typeName": "INT",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": False,
                    "isCurrency": False,
                    "isSigned": False,
                    "label": "name",
                    "name": "name",
                    "nullable": 1,
                    "precision": 255,
                    "scale": 0,
                    "schemaName": "",
                    "tableName": "users",
                    "type": 12,
                    "typeName": "VARCHAR",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": True,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "first_time",
                    "name": "first_time",
                    "nullable": 1,
                    "precision": 19,
                    "scale": 0,
                    "tableName": "pets",
                    "type": 91,
                    "typeName": "DATETIME",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": True,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "first_time",
                    "name": "first_time",
                    "nullable": 1,
                    "precision": 19,
                    "scale": 0,
                    "tableName": "pets",
                    "type": 93,
                    "typeName": "DATETIME",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": True,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "updated",
                    "name": "updated",
                    "nullable": 1,
                    "precision": 19,
                    "scale": 0,
                    "tableName": "pets",
                    "type": 93,
                    "typeName": "DATETIME",
                },
                {
                    "arrayBaseColumnType": 0,
                    "isAutoIncrement": False,
                    "isCaseSensitive": True,
                    "isCurrency": False,
                    "isSigned": True,
                    "label": "created",
                    "name": "created",
                    "nullable": 1,
                    "precision": 19,
                    "scale": 0,
                    "tableName": "pets",
                    "type": 93,
                    "typeName": "DATETIME",
                },
            ],
        },
        {"generatedFields": [], "numberOfRecordsUpdated": 1},
        {"generatedFields": [], "numberOfRecordsUpdated": 1},
    ]
    engine = create_engine(
        'postgresql+pydataapi://',
        connect_args={
            'resource_arn': 'arn:aws:rds:us-east-1:123456789012:cluster:dummy',
            'secret_arn': 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy',
            'database': 'test',
            'client': mocked_client,
        },
    )

    Session = sessionmaker(bind=engine)
    session = Session()
    result: List[Pets] = session.query(Pets).all()
    assert len(result) == 1
    assert result[0].id == 1
    assert result[0].name == 'cat'
    assert result[0].birthday == datetime.date(2019, 11, 11)
    assert result[0].first_time == datetime.datetime(2019, 11, 12, 10, 20, 20, 123456)
    # assert result[0].updated == datetime.datetime(2019, 11, 26, 3, 31, 40, 170858)
    assert result[0].created == datetime.datetime(2019, 11, 12, 10, 20, 30)

    pet = Pets(id=2, name='dog', created=datetime.datetime(2019, 11, 13, 10, 20, 30))
    session.add(pet)
    session.flush()

    pet = Pets(id=3, name='snake', created='2019-11-14 10:20:30')
    session.add(pet)
    session.flush()
