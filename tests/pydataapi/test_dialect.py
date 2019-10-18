import pytest
from sqlalchemy.engine import ResultProxy


@pytest.fixture
def mocked_client(mocker):
    return mocker.patch('boto3.client')


def test_mysql(mocked_client) -> None:
    from sqlalchemy.engine import create_engine

    mocked_client.begin_transaction.return_value = {'transactionId': 'abc'}
    mocked_client.execute_statement.side_effect = [
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
            'records': [[{'longValue': 1}, {'stringValue': 'cat'}]],
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
            ],
        },
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

    result: ResultProxy = engine.execute("select * from pets")
    assert result.fetchall() == [(1, 'cat')]


def test_postgresql(mocked_client) -> None:
    from sqlalchemy.engine import create_engine

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
            'records': [[{'longValue': 1}, {'stringValue': 'cat'}]],
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
            ],
        },
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

    result: ResultProxy = engine.execute("select * from pets")
    assert result.fetchall() == [(1, 'cat')]
